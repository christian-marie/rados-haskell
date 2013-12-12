-- |
-- The underlying Base methods used for the monadic implementation
-- "System.Rados.Monadic".
module System.Rados.Internal
(
-- *Type definitions
    ClusterHandle,
    Completion,
    IOContext,
-- *Connecting
    newClusterHandle,
    cleanupClusterHandle,
    confReadFile,
    connect,
-- *Writing
    newIOContext,
    cleanupIOContext,
-- **Synchronous
    syncWrite,
    syncWriteFull,
    syncRead,
    syncAppend,
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
) where

import qualified System.Rados.FFI as F
import System.Rados.Error(checkError, checkError_)

import Data.ByteString as B
import Foreign hiding (void)
import Foreign.C.String
import Foreign.C.Types
import Control.Applicative
import Control.Monad (void)

-- | An opaque pointer to a rados_t.
newtype ClusterHandle = ClusterHandle (Ptr F.RadosT)
-- | An opaque pointer to a rados_ioctx_t.
newtype IOContext     = IOContext (Ptr F.RadosIOCtxT)
-- | An opaque pointer to a rados_completion_t.
newtype Completion    = Completion (Ptr F.RadosCompletionT)

-- |
-- Attempt to create a new 'ClusterHandle, taking an optional id.
-- You must run cleanupClusterHandle on the handle when you are done with it.
--
-- @
-- h  <- newClusterHandle Nothing
-- h' <- newClusterHandle $ Just \"admin\"
--
-- cleanupClusterHandle h
-- cleanupClusterHandle h'
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_create>
newClusterHandle :: Maybe B.ByteString -> IO (ClusterHandle)
newClusterHandle maybe_bs =
    alloca $ \rados_t_ptr_ptr -> do
        checkError "c_rados_create" $ case maybe_bs of
            Nothing ->
                F.c_rados_create rados_t_ptr_ptr nullPtr
            Just bs -> B.useAsCString bs $ \cstr ->
                F.c_rados_create rados_t_ptr_ptr cstr
        ClusterHandle <$> peek rados_t_ptr_ptr

-- |
-- Clean up a cluster handle.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_shutdown>
cleanupClusterHandle :: ClusterHandle -> IO ()
cleanupClusterHandle (ClusterHandle rados_t_ptr) =
    F.c_rados_shutdown rados_t_ptr

-- |
-- Load a config specified by 'FilePath' into a given 'ClusterHandle.
--
-- @
-- h <- newClusterHandle Nothing
-- confReadFile h \"\/etc\/config\"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_conf_read_file>
confReadFile :: ClusterHandle -> FilePath -> IO ()
confReadFile (ClusterHandle rados_t_ptr) fp =
    checkError_ "c_rados_conf_read_file" $ withCString fp $ \cstr ->
        F.c_rados_conf_read_file rados_t_ptr cstr

-- |
-- Attempt to connect a configured 'ClusterHandle.
--
-- @
-- h <- newClusterHandle Nothing
-- confReadFile h \"/etc/config\"
-- connect h
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_connect>
connect :: ClusterHandle -> IO ()
connect (ClusterHandle rados_t_ptr) =
    checkError_ "c_rados_connect" $ F.c_rados_connect rados_t_ptr

-- |
-- Attempt to create a new 'IOContext, requires a valid 'ClusterHandle and
-- pool name.
--
-- @
-- h <- newClusterHandle Nothing
-- h ctx <- newIOContext h "thing"
-- cleanupIOContext ctx
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_create>
newIOContext :: ClusterHandle -> B.ByteString -> IO (IOContext)
newIOContext (ClusterHandle rados_t_ptr) bs = 
    B.useAsCString bs $ \cstr ->
    alloca $ \ioctx_ptr_ptr -> do
        checkError "c_rados_ioctx_create" $
            F.c_rados_ioctx_create rados_t_ptr cstr ioctx_ptr_ptr
        IOContext <$> peek ioctx_ptr_ptr

-- |
-- Clean up an IOContext
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_destroy>
cleanupIOContext :: IOContext -> IO ()
cleanupIOContext (IOContext ptr) = F.c_rados_ioctx_destroy ptr

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
        checkError "c_rados_aio_create_completion" $
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

-- |
-- From right to left, this function reads as:
--
-- Write ByteString bytes to Word64 offset at oid ByteString, notifying this
-- Completion, all within this IOContext
--
-- @
-- ...
-- ctx <- newIOContext h \"thing\"
-- n <- asyncWrite ctx c "oid" 42 \"written at offset fourty-two\"
-- putStrLn $ "I wrote " ++ show n ++ " bytes"
-- @
--
-- Calls rados_aio_write:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write>
asyncWrite :: IOContext
              -> Completion
              -> B.ByteString
              -> Word64
              -> B.ByteString
              -> IO Int
asyncWrite (IOContext ioctxt_ptr) (Completion rados_completion_t_ptr) oid offset bs =
    B.useAsCString oid        $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = CULLong offset
        checkError "c_rados_aio_write" $ F.c_rados_aio_write
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size c_offset
-- |
-- Same calling convention as asyncWrite, simply omitting an offset.
-- This will truncate any existing object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write_full>
asyncWriteFull :: IOContext
              -> Completion
              -> B.ByteString
              -> B.ByteString
              -> IO Int
asyncWriteFull (IOContext ioctxt_ptr) (Completion rados_completion_t_ptr) oid bs =
    B.useAsCString oid        $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError "c_rados_aio_write_full" $ 
            F.c_rados_aio_write_full
                ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size

-- |
-- Same calling convention as asyncWriteFull, simply appends to an object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_append>
asyncAppend :: IOContext
              -> Completion
              -> B.ByteString
              -> B.ByteString
              -> IO Int
asyncAppend (IOContext ioctxt_ptr) (Completion rados_completion_t_ptr) oid bs =
    B.useAsCString oid        $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError "c_rados_aio_append" $ F.c_rados_aio_append
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_size

-- |
--
-- From right to left, this function reads as:
-- Write ByteString buffer to Word64 offset at ByteString oid within this
-- IOContext
--
--
-- Usage is the same as 'asyncWrite', without a 'Completion.
--
-- @
-- ...
-- n <- syncWrite c \"oid\" 42 \"written at offset fourty-two\"
-- @
--
-- Returns the number of bytes written.
--
-- Calls rados_aio_write:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_write>
syncWrite :: IOContext
         -> B.ByteString
         -> Word64
         -> B.ByteString
         -> IO Int
syncWrite (IOContext ioctxt_ptr) oid offset bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = CULLong offset
        checkError "c_rados_write" $ F.c_rados_write
            ioctxt_ptr c_oid c_buf c_size c_offset

-- |
-- The same as 'syncWrite', but omitting an offset and truncating any object that
-- already exists with that oid.
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_write_full>
syncWriteFull :: IOContext
         -> B.ByteString
         -> B.ByteString
         -> IO Int
syncWriteFull (IOContext ioctxt_ptr) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, len) -> do
        checkError "c_rados_write_full" $ F.c_rados_write_full
            ioctxt_ptr c_oid c_buf len

-- |
-- Same calling convention as 'syncWriteFull', appends to an object.
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_append>
syncAppend :: IOContext
         -> B.ByteString
         -> B.ByteString
         -> IO Int
syncAppend (IOContext ioctxt_ptr) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        checkError "c_rados_append" $ F.c_rados_append
            ioctxt_ptr c_oid c_buf c_size

-- |
-- Read length bytes into a ByteString, using context and oid.
--
-- There is no async read provided by this binding.
--
-- context -> oid -> length -> offset -> IO read_bytes
--
-- @
-- ...
-- bs <- syncRead ctx \"oid\" 100 42
-- @
--
-- The above will place 100 bytes into bs, read from an offset of 42
syncRead :: IOContext
    -> B.ByteString
    -> Word64
    -> Word64
    -> IO B.ByteString
syncRead (IOContext ioctxt_ptr) oid offset len =
    allocaBytes (fromIntegral len) $ \c_buf ->
    B.useAsCString oid             $ \c_oid -> do
        let c_offset = CULLong offset
        let c_len    = CSize  len
        read_bytes <- checkError "c_rados_read" $ F.c_rados_read
            ioctxt_ptr c_oid c_buf c_len c_offset
        B.packCStringLen (c_buf, read_bytes)

useAsCStringCSize :: ByteString -> ((CString, CSize) -> IO a) -> IO a 
useAsCStringCSize bs f =
    B.useAsCStringLen bs $ \(cstr, len) -> f (cstr, (CSize . fromIntegral) len)
