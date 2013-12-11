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
data IOCtxT

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
			    -> Ptr (Ptr IOCtxT)
			    -> IO Errno

foreign import ccall unsafe "librados.h &rados_ioctx_destroy"
   c_rados_ioctx_destroy :: FunPtr (Ptr IOCtxT -> IO ())

foreign import ccall unsafe "string.h"
    c_strerror :: Errno -> IO (Ptr CChar)
