{-# LANGUAGE ForeignFunctionInterface, EmptyDataDecls #-}
module System.Rados.FFI where

import Foreign
import Foreign.C.Types
import Foreign.C.String
import Foreign.C.Error

#include <rados/librados.h>

-- typedef void *rados_t;
data RadosT

-- typedef void *rados_ioctx_t;
data RadosIOCtxT

-- typedef void *rados_completion_t;
data RadosCompletionT

-- typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);
type RadosCallback  = Ptr RadosCompletionT -> Ptr () -> IO ()
type RadosCallbackT = FunPtr RadosCallback

foreign import ccall unsafe "librados.h rados_create"
    c_rados_create :: Ptr (Ptr RadosT) -> CString -> IO Errno

foreign import ccall unsafe "librados.h &rados_shutdown"
    c_rados_shutdown :: FunPtr (Ptr RadosT -> IO ())

foreign import ccall unsafe "librados.h rados_conf_read_file"
    c_rados_conf_read_file :: Ptr RadosT -> CString -> IO Errno

foreign import ccall unsafe "librados.h rados_conf_read_file"
    c_rados_connect :: Ptr RadosT -> IO Errno

foreign import ccall unsafe "librados.h rados_ioctx_create"
    c_rados_ioctx_create :: Ptr RadosT
    			    -> CString
			    -> Ptr (Ptr RadosIOCtxT)
			    -> IO Errno

foreign import ccall unsafe "librados.h &rados_ioctx_destroy"
    c_rados_ioctx_destroy :: FunPtr (Ptr RadosIOCtxT -> IO ())

foreign import ccall unsafe "wrapper"
    c_wrap_callback :: RadosCallback -> IO RadosCallbackT

foreign import ccall unsafe "librados.h rados_aio_create_completion"
    c_rados_aio_create_completion :: Ptr ()
				   -> RadosCallbackT
				   -> RadosCallbackT
				   -> Ptr (Ptr RadosCompletionT)
				   -> IO Errno

foreign import ccall unsafe "librados.h &rados_aio_release"
    c_rados_aio_release :: FunPtr (Ptr RadosCompletionT -> IO ())

foreign import ccall unsafe "string.h strerror"
    c_strerror :: Errno -> IO (Ptr CChar)
