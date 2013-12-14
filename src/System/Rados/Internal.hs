-- |
-- The underlying Base methods used for the monadic implementation
-- "System.Rados.Monadic".
module System.Rados.Internal
(
-- *Type definitions
    Connection,
    Completion,
    Pool,
-- *Connecting
    newConnection,
    cleanupConnection,
    confReadFile,
    connect,
-- *Writing
    newPool,
    cleanupPool,
-- **Synchronous
    syncWrite,
    syncWriteFull,
    syncRead,
    syncAppend,
    syncRemove,
-- **Asynchronous
    newCompletion,
    cleanupCompletion,
    waitForComplete,
    waitForSafe,
    isSafe,
    isComplete,
    asyncWrite,
    asyncWriteFull,
    asyncAppend,
    getAsyncError,
) where

import System.Rados.Error (checkError, checkError_)
import qualified System.Rados.Error as E
import qualified System.Rados.FFI as F

import Control.Applicative
import Control.Monad (void)
import Data.ByteString as B
import Foreign hiding (Pool, newPool, void)
import Foreign.C.String
import Foreign.C.Types

-- | A connection to a rados cluster, required to get a 'Pool'
newtype Connection = Connection (Ptr F.RadosT)
-- | An IO context with a rados pool.
newtype Pool     = Pool (Ptr F.RadosIOCtxT)
-- | A handle to query the status of an asynchronous action
newtype Completion    = Completion (Ptr F.RadosCompletionT)

-- |
-- Attempt to create a new 'Connection, taking an optional id.
-- You must run cleanupConnection on the handle when you are done with it.
--
-- @
-- h  <- newConnection Nothing
-- h' <- newConnection $ Just \"admin\"
--
-- cleanupConnection h
-- cleanupConnection h'
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_create>
newConnection :: Maybe B.ByteString -> IO (Connection)
newConnection maybe_bs =
    alloca $ \rados_t_ptr_ptr -> do
        checkError "rados_create" $ case maybe_bs of
            Nothing ->
                F.c_rados_create rados_t_ptr_ptr nullPtr
            Just bs -> B.useAsCString bs $ \cstr ->
                F.c_rados_create rados_t_ptr_ptr cstr
        Connection <$> peek rados_t_ptr_ptr

-- |
-- Clean up a cluster handle.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_shutdown>
cleanupConnection :: Connection -> IO ()
cleanupConnection (Connection rados_t_ptr) =
    F.c_rados_shutdown rados_t_ptr

-- |
-- Load a config specified by 'FilePath' into a given 'Connection.
--
-- @
-- h <- newConnection Nothing
-- confReadFile h \"\/etc\/config\"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_conf_read_file>
confReadFile :: Connection -> FilePath -> IO ()
confReadFile (Connection rados_t_ptr) fp =
    checkError_ "rados_conf_read_file" $ withCString fp $ \cstr ->
        F.c_rados_conf_read_file rados_t_ptr cstr

-- |
-- Attempt to connect a configured 'Connection.
--
-- @
-- h <- newConnection Nothing
-- confReadFile h \"/etc/config\"
-- connect h
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_connect>
connect :: Connection -> IO ()
connect (Connection rados_t_ptr) =
    checkError_ "rados_connect" $ F.c_rados_connect rados_t_ptr

-- |
-- Attempt to create a new 'Pool, requires a valid 'Connection and
-- pool name.
--
-- @
-- h <- newConnection Nothing
-- h ctx <- newPool h "thing"
-- cleanupPool ctx
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_create>
newPool :: Connection -> B.ByteString -> IO (Pool)
newPool (Connection rados_t_ptr) bs =
    B.useAsCString bs $ \cstr ->
    alloca $ \ioctx_ptr_ptr -> do
        checkError "rados_ioctx_create" $
            F.c_rados_ioctx_create rados_t_ptr cstr ioctx_ptr_ptr
        Pool <$> peek ioctx_ptr_ptr

-- |
-- Clean up an Pool
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_destroy>
cleanupPool :: Pool -> IO ()
cleanupPool (Pool ptr) = F.c_rados_ioctx_destroy ptr

-- |
-- Attempt to create a new 'Completion' that can be used with async IO actions.
--
-- You will need to run cleanupCompletion on the 'Completion' when you are done
-- with it.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_create_completion>
newCompletion :: IO Completion
newCompletion =
    alloca $ \completion_ptr_ptr -> do
        checkError "rados_aio_create_completion" $
            F.c_rados_aio_create_completion nullPtr nullFunPtr nullFunPtr completion_ptr_ptr
        Completion <$> peek completion_ptr_ptr

-- |
-- Cleanup a 'Completion'
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_release>
cleanupCompletion :: Completion -> IO ()
cleanupCompletion (Completion rados_completion_t_ptr) =
    F.c_rados_aio_release rados_completion_t_ptr

-- |
-- Block until a completion is complete. I.e. the operation associated with
-- the completion is at least in memory on all replicas.
--
-- @
-- ...
-- asyncWrite ctx c "oid" 42 \"written at offset fourty-two\"
-- putStrLn \"Waiting for your bytes to get there...\"
-- waitForComplete c
-- putStrLn \"I totally wrote it! Maybe.\"
-- @
--
-- Calls rados_aio_wait_for_complete:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_complete>
waitForComplete :: Completion -> IO ()
waitForComplete (Completion rados_completion_t_ptr) = void $
    F.c_rados_aio_wait_for_complete rados_completion_t_ptr

-- |
-- Block until a completion is safe. I.e. the operation associated with
-- the completion is on stable storage on all replicas.
--
-- @
-- ...
-- waitForSafe c
-- putStrLn \"Yeah. Totally did write it. I hope.\"
-- @
--
-- Calls rados_aio_wait_for_safe:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_safe>
waitForSafe :: Completion -> IO ()
waitForSafe (Completion rados_completion_t_ptr) = void $
    F.c_rados_aio_wait_for_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in memory on all replicas?
--
--
--
-- Cals rados_aio_is_complete:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_complete>
isComplete :: Completion -> IO (Bool)
isComplete (Completion rados_completion_t_ptr) =
    (/= 0) <$> F.c_rados_aio_is_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in stable storage on all
-- replicas?
--
-- Calls rados_aio_is_safe:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_safe>
isSafe :: Completion -> IO (Bool)
isSafe (Completion rados_completion_t_ptr) =
    (/= 0) <$> F.c_rados_aio_is_safe rados_completion_t_ptr


getAsyncError :: Completion -> IO (Maybe E.RadosError)
getAsyncError (Completion rados_completion_t_ptr) = do
    result <- E.checkError' "rados_aio_get_return_value" $ F.c_rados_aio_get_return_value rados_completion_t_ptr
    return $ either Just (\_ -> Nothing) result


-- |
-- From right to left, this function reads as:
--
-- Write ByteString bytes to Word64 offset at oid ByteString, notifying this
-- Completion, all within this Pool
--
-- @
-- ...
-- ctx <- newPool h \"thing\"
-- asyncWrite ctx c "oid" 42 \"written at offset fourty-two\"
-- putStrLn $ "I wrote " ++ show n ++ " bytes"
-- @
--
-- Calls rados_aio_write:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write>
asyncWrite
    :: Pool
    -> Completion
    -> B.ByteString
    -> Word64
    -> B.ByteString
    -> IO ()
asyncWrite (Pool ioctxt_ptr) (Completion rados_completion_t_ptr)
           oid offset bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = CULLong offset
        checkError_ "rados_aio_write" $ F.c_rados_aio_write
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size c_offset
-- |
-- Same calling convention as asyncWrite, simply omitting an offset.
-- This will truncate any existing object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write_full>
asyncWriteFull
    :: Pool
    -> Completion
    -> B.ByteString
    -> B.ByteString
    -> IO ()
asyncWriteFull (Pool ioctxt_ptr) (Completion rados_completion_t_ptr) oid bs =
    B.useAsCString oid        $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError_ "rados_aio_write_full" $
            F.c_rados_aio_write_full
                ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size

-- |
-- Same calling convention as asyncWriteFull, simply appends to an object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_append>
asyncAppend
    :: Pool
    -> Completion
    -> B.ByteString
    -> B.ByteString
    -> IO ()
asyncAppend (Pool ioctxt_ptr) (Completion rados_completion_t_ptr) oid bs =
    B.useAsCString oid        $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError_ "rados_aio_append" $ F.c_rados_aio_append
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size

-- |
-- Write a 'ByteString' to 'Pool', object id and offset.
--
-- @
-- ...
-- syncWrite Pool \"object_id\" 42 \"written at offset fourty-two\"
-- @
syncWrite
    :: Pool
    -> B.ByteString
    -> Word64
    -> B.ByteString
    -> IO ()
syncWrite (Pool ioctxt_ptr) oid offset bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = CULLong offset
        checkError_ "rados_write" $ F.c_rados_write
            ioctxt_ptr c_oid c_buf c_size c_offset

-- |
-- Write a 'ByteString' to 'Pool' and object id.
--
-- This will replace any existing object at the same 'Pool' and object id.
syncWriteFull
    :: Pool
    -> B.ByteString
    -> B.ByteString
    -> IO ()
syncWriteFull (Pool ioctxt_ptr) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, len) -> do
        checkError_ "rados_write_full" $ F.c_rados_write_full
            ioctxt_ptr c_oid c_buf len

-- |
-- Append a 'ByteString' to 'Pool' and object id.
--
-- Returns the number of bytes written.
syncAppend
    :: Pool
    -> B.ByteString
    -> B.ByteString
    -> IO ()
syncAppend (Pool ioctxt_ptr) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError_ "rados_append" $ F.c_rados_append
            ioctxt_ptr c_oid c_buf c_size

-- |
-- Read from 'Pool', object ID and offset n bytes.
--
-- There is no async read provided by this binding.
--
-- TODO: Document a multithreaded example to compensate for no async. This may
-- or may not require to be called from the same OS thread. Test and document
-- that.
--
-- Or, even, provide an abstract Future type that can simply wrap an mvar, then
-- do the multithreading for the user.
--
-- @
--         ...
--         -- Read 100 bytes into bs from offset 42
--         bs <- syncRead pool \"object_id\" 42 100
--         ...
-- @
syncRead
    :: Pool
    -> B.ByteString
    -> Word64
    -> Word64
    -> IO B.ByteString
syncRead (Pool ioctxt_ptr) oid offset len =
    allocaBytes (fromIntegral len) $ \c_buf ->
    B.useAsCString oid             $ \c_oid -> do
        let c_offset = CULLong offset
        let c_len    = CSize  len
        read_bytes <- checkError "rados_read" $ F.c_rados_read
            ioctxt_ptr c_oid c_buf c_len c_offset
        B.packCStringLen (c_buf, read_bytes)

useAsCStringCSize :: ByteString -> ((CString, CSize) -> IO a) -> IO a
useAsCStringCSize bs f =
    B.useAsCStringLen bs $ \(cstr, len) -> f (cstr, (CSize . fromIntegral) len)

-- |
-- Delete an object from 'Pool' by ID.
--
syncRemove
    :: Pool
    -> B.ByteString
    -> IO ()
syncRemove (Pool ioctxt_ptr) oid =
    B.useAsCString oid   $ \c_oid ->
        checkError_ "rados_remove" $ F.c_rados_remove
            ioctxt_ptr c_oid
