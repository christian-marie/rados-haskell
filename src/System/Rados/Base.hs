-- |
-- The underlying Base methods used for the monadic implementation
-- "System.Rados.Monadic".
--
-- It is not recommended to use this module directly.
module System.Rados.Base
(
-- *Type definitions
    ClusterHandle,
    Completion,
    IOContext,
-- *Connecting
    newClusterHandle,
    confReadFile,
    connect,
-- *Writing
    newIOContext,
-- **Synchronous
    syncWrite,
    syncWriteFull,
    syncRead,
-- **Asynchronous
    newCompletion,
    waitForComplete,
    waitForSafe,
    isSafe,
    isComplete,
    asyncWrite,
) where

import qualified System.Rados.FFI as F
import Data.ByteString as B
import Foreign hiding (void)
import Foreign.C.String
import Foreign.C.Error
import Foreign.C.Types
import Control.Applicative
import Control.Monad (void)

-- An opaque pointer to a rados_t structure.
type ClusterHandle = ForeignPtr F.RadosT
type IOContext     = ForeignPtr F.RadosIOCtxT
type Completion    = ForeignPtr F.RadosCompletionT

-- |
-- Attempt to create a new 'ClusterHandle', taking an optional id.
--
-- @
-- h  <- newClusterHandle Nothing
-- h' <- newClusterHandle \"admin\"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_create>
--
-- The ClusterHandle returned will have rados_shutdown automatically run when
-- it is garbage collected.
--
-- Cleans up with:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_shutdown>
newClusterHandle :: Maybe B.ByteString -> IO (ClusterHandle)
newClusterHandle maybe_bs = do
    -- Allocate a void pointer to cast to our Ptr RadostT
    radost_t_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
    checkError "c_rados_create" $ case maybe_bs of
        Nothing ->
            F.c_rados_create radost_t_ptr nullPtr
        Just bs -> B.useAsCString bs $ \cstr ->
            F.c_rados_create radost_t_ptr cstr
    -- Call shutdown on GC, this can't be called more than once or an assert()
    -- freaks out.
    fp <- newForeignPtr finalizerFree =<< peek radost_t_ptr
    addForeignPtrFinalizer F.c_rados_shutdown fp 
    return fp

-- |
-- Load a config specified by 'FilePath' into a given 'ClusterHandle'.
--
-- @
-- h <- newClusterHandle Nothing
-- confReadFile h \"/etc/config\"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_conf_read_file>
confReadFile :: ClusterHandle -> FilePath -> IO ()
confReadFile handle fp = void $
    withForeignPtr handle $ \rados_t_ptr ->
        checkError "c_rados_conf_read_file" $ withCString fp $ \cstr ->
            F.c_rados_conf_read_file rados_t_ptr cstr

-- |
-- Attempt to connect a configured 'ClusterHandle'.
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
connect handle = void $
    withForeignPtr handle $ \rados_t_ptr ->
        checkError "c_rados_connect" $ F.c_rados_connect rados_t_ptr

-- |
-- Attempt to create a new 'IOContext', requires a valid 'ClusterHandle' and
-- pool name.
--
-- @
-- h <- newClusterHandle Nothing
-- h ctx <- newIOContext h "thing"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_create>
--
-- And on cleanup:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_destroy>
newIOContext :: ClusterHandle -> B.ByteString -> IO (IOContext)
newIOContext handle bs = B.useAsCString bs $ \cstr -> do
    withForeignPtr handle $ \rados_t_ptr -> do
        ioctxt_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
        checkError "c_rados_ioctx_create" $
            F.c_rados_ioctx_create rados_t_ptr cstr ioctxt_ptr
        fp <- newForeignPtr finalizerFree =<< peek ioctxt_ptr
        addForeignPtrFinalizer F.c_rados_ioctx_destroy fp
        return fp

-- |
-- Attempt to create a new completion that can be used with async IO actions.
--
-- This completion will be released automatically when it is garbage collected.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_create_completion>
--
-- And on cleanup:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_release>
newCompletion :: IO Completion
newCompletion = do
    completion_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
    checkError "c_rados_aio_create_completion" $
        F.c_rados_aio_create_completion nullPtr nullFunPtr nullFunPtr completion_ptr
    fp <- newForeignPtr finalizerFree =<< peek completion_ptr
    addForeignPtrFinalizer F.c_rados_aio_release fp
    return fp

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
waitForComplete completion = void $
    withForeignPtr completion $ \rados_completion_t_ptr ->
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
waitForSafe completion = void $
    withForeignPtr completion $ \rados_completion_t_ptr ->
        F.c_rados_aio_wait_for_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in memory on all replicas?
--
--
--
-- Cals rados_aio_is_complete:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_complete>
isComplete :: Completion -> IO (Bool)
isComplete completion = withForeignPtr completion $ \rados_completion_t_ptr ->
        (/= 0) <$> F.c_rados_aio_is_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in stable storage on all
-- replicas?
--
-- Calls rados_aio_is_safe:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_safe>
isSafe :: Completion -> IO (Bool)
isSafe completion = withForeignPtr completion $ \rados_completion_t_ptr ->
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
asyncWrite ioctx completion oid offset bs =
    withForeignPtr ioctx      $ \ioctxt_ptr ->
    withForeignPtr completion $ \rados_completion_t_ptr ->
    B.useAsCString oid        $ \c_oid ->
    B.useAsCStringLen bs      $ \(c_buf, len) -> do
        let c_offset = CULLong offset
        let c_len    = CSize $ fromIntegral len
        (Errno n) <- checkError "c_rados_aio_write" $ F.c_rados_aio_write
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_len c_offset
        return $ fromIntegral n
-- |
--
-- From right to left, this function reads as:
-- Write ByteString buffer to Word64 offset at ByteString oid within this
-- IOContext
--
--
-- Usage is the same as asyncWrite, without a context.
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
syncWrite ioctx oid offset bs =
    withForeignPtr ioctx $ \ioctxt_ptr ->
    B.useAsCString oid   $ \c_oid ->
    B.useAsCStringLen bs $ \(c_buf, len) -> do
        let c_offset = CULLong offset
        let c_len    = CSize $ fromIntegral len
        (Errno n) <- checkError "c_rados_write" $ F.c_rados_write
            ioctxt_ptr c_oid c_buf c_len c_offset
        return $ fromIntegral n

-- |
-- The same as syncWrite, but omitting an offset and truncating any object that
-- already exists with that oid.
syncWriteFull :: IOContext
         -> B.ByteString
         -> B.ByteString
         -> IO Int
syncWriteFull ioctx oid bs =
    withForeignPtr ioctx $ \ioctxt_ptr ->
    B.useAsCString oid   $ \c_oid ->
    B.useAsCStringLen bs $ \(c_buf, len) -> do
        let c_len    = CSize $ fromIntegral len
        (Errno n) <- checkError "c_rados_write_full" $ F.c_rados_write_full
            ioctxt_ptr c_oid c_buf c_len 
        return $ fromIntegral n


-- |
-- Read length bytes into a ByteString, using context and oid.
--
-- There is no async read provided by this binding.
--
-- context -> oid -> length -> offset -> IO read_bytes
--
-- @
-- ...
-- bs <- syncRead ctx "oid" 100 42
-- @
--
-- The above will place 100 bytes into bs, read from an offset of 42
syncRead :: IOContext
    -> B.ByteString
    -> Word64
    -> Word64
    -> IO B.ByteString
syncRead ioctx oid offset len =
    withForeignPtr ioctx           $ \ioctxt_ptr ->
    allocaBytes (fromIntegral len) $ \c_buf ->
    B.useAsCString oid             $ \c_oid -> do
        let c_offset = CULLong offset
        let c_len    = CSize  len
        checkError "c_rados_read" $ F.c_rados_read
            ioctxt_ptr c_oid c_buf c_len c_offset
        B.packCString c_buf

-- Handle a ceph Errno, which is an errno that must be negated before being
-- passed to strerror.
checkError :: String -> IO Errno -> IO Errno
checkError desc action = do
    e@(Errno n) <- action
    if n < 0
        then do
            let negated = Errno (-n)
            strerror <- peekCString =<< F.c_strerror negated
            error $ desc ++ ": " ++ strerror
        else return e
