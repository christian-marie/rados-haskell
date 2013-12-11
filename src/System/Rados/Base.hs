module System.Rados.Base
(
    newClusterHandle,
    confReadFile,
    connect,
    newIOContext,
    newCompletion',
    newCompletion
) where

import qualified System.Rados.FFI as F
import Data.ByteString as B
import Foreign hiding (void)
import Foreign.C.String
import Foreign.C.Error
import Control.Applicative
import Control.Monad (void)

-- An opaque pointer to a rados_t structure.
type ClusterHandle = ForeignPtr F.RadosT
type IOContext     = ForeignPtr F.RadosIOCtxT
type Completion    = ForeignPtr F.RadosCompletionT -- TODO, confirm that librados cleans this up

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

-- Attempt to create a new completion without any callbacks.
newCompletion :: IO Completion
newCompletion = newCompletion' Nothing Nothing

-- |
-- TODO: This function currently leaks memory due to not cleaning up generated
-- FunPtrs. It needs to pass the FunPtrs through to void *args of
-- c_rados_aio_create_completion and then wrap the generated callback with a
-- call to freeHaskellFunPtr. There are more pressing things to do currently,
-- so don't use callbacks unless leaky memory is okay.
--
-- Attempt to create a new completion that can be used with async IO actions.
-- This completion will be released automatically when it is garbage collected.
--
-- Optional callbacks are for complete and safe states respectively. 
--
-- Complete means that the operation is in memory on all replicas.
-- 
-- Safe means that the operation is on stable storage on all replicas.
--
-- Calls rados_aio_create_completion:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_create_completion
--
-- And rados_aio_release on cleanup:
-- http://ceph.com/docs/master/rados/api/librados/#rados_aio_release
newCompletion' :: Maybe (IO ()) -> Maybe (IO ()) -> IO Completion
newCompletion' complete_cb safe_cb = do
    completion_ptr <- castPtr <$> (malloc :: IO (Ptr WordPtr))
    w_complete_cb <- wrap complete_cb
    w_safe_cb <- wrap safe_cb
    checkError "c_rados_aio_create_completion" $
        F.c_rados_aio_create_completion nullPtr w_complete_cb w_safe_cb completion_ptr
    newForeignPtr F.c_rados_aio_release =<< peek completion_ptr
  where
    wrap ma     = maybe (return $ nullFunPtr) mkFunPtr ma
    mkFunPtr a  = F.c_wrap_callback (\_ _ -> a )

-- Handle a ceph Errno, which is an errno that must be negated before being
-- passed toi strerror.
checkError :: String -> IO Errno -> IO Errno
checkError desc action = do
    e@(Errno n) <- action
    if n < 0
        then do
            let negated = Errno (-n)
            strerror <- peekCString =<< F.c_strerror negated
            error $ desc ++ ": " ++ strerror
        else return e
