module System.Rados.Base
(
    newClusterHandle,
    confReadFile,
    connect,
    newIOContext,
    newCompletion,
    waitForComplete,
    waitForSafe,
    isSafe,
    isComplete,
    asyncWrite,
    syncWrite,
    syncRead,
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
-- Attempt to create a new ClusterHandle, taking an optional id.
--
-- Calls rados_create:
-- http://ceph.com/docs/master/rados/api/librados/#rados_create
--
-- The ClusterHandle returned will have rados_shutdown run when it is garbage
-- collected.
-- Calls rados_shutdown:
-- http://ceph.com/docs/master/rados/api/librados/#rados_shutdown
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
    newForeignPtr F.c_rados_shutdown =<< peek radost_t_ptr

-- |
-- Configure a ClusterHandle from a config file.
--
-- Will load a config specified by FilePath into ClusterHandle.
--
-- Calls rados_conf_read_file:
-- http://ceph.com/docs/master/rados/api/librados/#rados_conf_read_file
confReadFile :: ClusterHandle -> FilePath -> IO ()
confReadFile handle fp = void $
    withForeignPtr handle $ \rados_t_ptr ->
        checkError "c_rados_conf_read_file" $ withCString fp $ \cstr ->
            F.c_rados_conf_read_file rados_t_ptr cstr

-- |
-- Attempt to connect a configured ClusterHandle.
--
-- Calls rados_connect
-- http://ceph.com/docs/master/rados/api/librados/#rados_connect
connect :: ClusterHandle -> IO ()
connect handle = void $ 
    withForeignPtr handle $ \rados_t_ptr ->
        checkError "c_rados_connect" $ F.c_rados_connect rados_t_ptr

-- |
-- Attempt to create a new IOContext, requires a valid ClusterHandle and pool
-- name.
--
-- Calls c_rados_ioctx_create:
-- http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_create
--
-- Calls c_rados_ioctx_destroy on garbage collection:
-- http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_destroy
newIOContext :: ClusterHandle -> B.ByteString -> IO (IOContext)
newIOContext handle bs = B.useAsCString bs $ \cstr -> do
    withForeignPtr handle $ \rados_t_ptr -> do
        ioctxt_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
        checkError "c_rados_ioctx_create" $ 
            F.c_rados_ioctx_create rados_t_ptr cstr ioctxt_ptr
        newForeignPtr F.c_rados_ioctx_destroy =<< peek ioctxt_ptr

-- |
-- Attempt to create a new completion that can be used with async IO actions.
-- This completion will be released automatically when it is garbage collected.
--
-- Calls rados_aio_create_completion:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_create_completion
--
-- And rados_aio_release on cleanup:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_release
newCompletion :: IO Completion
newCompletion = do
    completion_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
    checkError "c_rados_aio_create_completion" $
        F.c_rados_aio_create_completion nullPtr nullFunPtr nullFunPtr completion_ptr
    newForeignPtr F.c_rados_aio_release =<< peek completion_ptr

-- |
-- Block until a completion is complete. I.e. the operation associated with
-- the completion is at least in memory on all replicas.
--
-- Calls rados_aio_wait_for_complete:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_complete
waitForComplete :: Completion -> IO ()
waitForComplete completion = void $
    withForeignPtr completion $ \rados_completion_t_ptr ->
        F.c_rados_aio_wait_for_complete rados_completion_t_ptr
        
-- |
-- Block until a completion is safe. I.e. the operation associated with
-- the completion is on stable storage on all replicas.
--
-- Calls rados_aio_wait_for_safe:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_safe
waitForSafe :: Completion -> IO ()
waitForSafe completion = void $
    withForeignPtr completion $ \rados_completion_t_ptr ->
        F.c_rados_aio_wait_for_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in memory on all replicas?
--
-- Cals rados_aio_is_complete:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_complete
isComplete :: Completion -> IO (Bool)
isComplete completion = withForeignPtr completion $ \rados_completion_t_ptr ->
        (/= 0) <$> F.c_rados_aio_is_complete rados_completion_t_ptr

-- |
-- Is the operation associated with this completion in stable storage on all
-- replicas?
--
-- Calls rados_aio_is_safe:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_is_safe
isSafe :: Completion -> IO (Bool)
isSafe completion = withForeignPtr completion $ \rados_completion_t_ptr ->
        (/= 0) <$> F.c_rados_aio_is_safe rados_completion_t_ptr

-- |
-- From right to left, this function reads as:
-- "Write ByteString bytes to Word64 offset at oid ByteString, notifying this
-- Completion, all within this IOContext"
--
-- Returns the number of bytes written.
-- 
-- Calls rados_aio_write:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_write
asyncWrite :: IOContext
              -> Completion
              -> B.ByteString
              -> Word64
              -> B.ByteString
              -> IO Int
asyncWrite ioctx completion oid offset bs =
    withForeignPtr ioctx $ \ioctxt_ptr ->
    withForeignPtr completion $ \rados_completion_t_ptr ->
    B.useAsCString oid $ \c_oid ->
    B.useAsCStringLen bs $ \(c_buf, len) -> do
        let c_offset = CULLong offset
        let c_len    = CSize $ fromIntegral len
        (Errno n) <- checkError "c_rados_aio_write" $ F.c_rados_aio_write
            ioctxt_ptr c_oid rados_completion_t_ptr c_buf c_len c_offset
        return $ fromIntegral n
-- |
-- From right to left, this function reads as:
-- "Write ByteString bytes to Word64 offset at oid ByteString within this
-- IOContext"
--
-- Returns the number of bytes written.
-- 
-- Calls rados_aio_write:
-- http://ceph.com/docs/master/rados/api/librados/#rados_write
syncWrite :: IOContext
         -> B.ByteString
         -> Word64
         -> B.ByteString
         -> IO Int
syncWrite ioctx oid offset bs =
    withForeignPtr ioctx $ \ioctxt_ptr ->
    B.useAsCString oid $ \c_oid ->
    B.useAsCStringLen bs $ \(c_buf, len) -> do
        let c_offset = CULLong offset
        let c_len    = CSize $ fromIntegral len
        (Errno n) <- checkError "c_rados_write" $ F.c_rados_write
            ioctxt_ptr c_oid c_buf c_len c_offset
        return $ fromIntegral n

-- |
-- Read length bytes into a ByteString, using context and oid.
--
-- context -> oid -> length -> offset -> IO read_bytes
syncRead :: IOContext
    -> B.ByteString
    -> Word64
    -> Word64
    -> IO B.ByteString
syncRead ioctx oid offset len =
    withForeignPtr ioctx $ \ioctxt_ptr ->
    allocaBytes (fromIntegral len) $ \c_buf ->
    B.useAsCString oid $ \c_oid -> do
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
