{-# LANGUAGE EmptyDataDecls           #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module System.Rados.FFI where

import Foreign
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types

#include <rados/librados.h>
#include <sys/time.h>

-- typedef void *rados_t;
data RadosT

-- typedef void *rados_ioctx_t;
data RadosIOCtxT

-- typedef void *rados_completion_t;
data RadosCompletionT

newtype LockFlag = LockFlag { unWrap :: Word8 }
#{enum LockFlag, LockFlag, idempotent = LIBRADOS_LOCK_FLAG_RENEW }

-- typedef void (*rados_callback_t)(rados_completion_t cb, void *arg);
type RadosCallback  = Ptr RadosCompletionT -> Ptr () -> IO ()
type RadosCallbackT = FunPtr RadosCallback


data TimeVal = TimeVal
    { seconds      :: CLong 
    , microseconds :: CLong
    } deriving (Eq, Show)

-- http://www.haskell.org/haskellwiki/FFICookBook#Working_with_structs
#{def typedef struct timeval timeval_typedef;}
#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)

instance Storable TimeVal where
    alignment _ = #{alignment timeval_typedef}
    sizeOf    _ = #{size timeval_typedef}
    peek p = do
        seconds  <-  #{peek timeval_typedef, tv_sec} p
        microseconds  <- #{peek timeval_typedef, tv_usec} p
        return $ TimeVal seconds microseconds
    poke p (TimeVal sec usec) = do
        #{poke timeval_typedef, tv_sec} p sec
        #{poke timeval_typedef, tv_usec} p usec

foreign import ccall unsafe "librados.h rados_create"
    c_rados_create :: Ptr (Ptr RadosT) -> CString -> IO CInt

foreign import ccall unsafe "librados.h rados_shutdown"
    c_rados_shutdown :: Ptr RadosT -> IO ()

foreign import ccall unsafe "librados.h rados_conf_read_file"
    c_rados_conf_read_file :: Ptr RadosT -> CString -> IO CInt

foreign import ccall unsafe "librados.h rados_connect"
    c_rados_connect :: Ptr RadosT -> IO CInt

foreign import ccall unsafe "librados.h rados_ioctx_create"
    c_rados_ioctx_create
        :: Ptr RadosT
        -> CString
        -> Ptr (Ptr RadosIOCtxT)
        -> IO CInt

foreign import ccall unsafe "librados.h rados_ioctx_destroy"
    c_rados_ioctx_destroy :: Ptr RadosIOCtxT -> IO ()

foreign import ccall unsafe "wrapper"
    c_wrap_callback :: RadosCallback -> IO RadosCallbackT

foreign import ccall unsafe "librados.h rados_aio_create_completion"
    c_rados_aio_create_completion
        :: Ptr ()
        -> RadosCallbackT
        -> RadosCallbackT
        -> Ptr (Ptr RadosCompletionT)
        -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_release"
    c_rados_aio_release :: Ptr RadosCompletionT -> IO ()

foreign import ccall unsafe "string.h strerror"
    c_strerror :: Errno -> IO (Ptr CChar)

foreign import ccall unsafe "librados.h rados_aio_wait_for_complete"
    c_rados_aio_wait_for_complete :: Ptr RadosCompletionT -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_wait_for_safe"
    c_rados_aio_wait_for_safe :: Ptr RadosCompletionT -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_is_complete"
    c_rados_aio_is_complete :: Ptr RadosCompletionT -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_is_safe"
    c_rados_aio_is_safe :: Ptr RadosCompletionT -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_get_return_value"
    c_rados_aio_get_return_value :: Ptr RadosCompletionT -> IO CInt

foreign import ccall unsafe "librados.h rados_aio_write"
    c_rados_aio_write
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> CULLong
        -> IO CInt


foreign import ccall unsafe "librados.h rados_aio_write_full"
    c_rados_aio_write_full
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> IO CInt


foreign import ccall unsafe "librados.h rados_aio_append"
    c_rados_aio_append
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall unsafe "librados.h rados_write"
    c_rados_write :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> CULLong
        -> IO CInt

foreign import ccall unsafe "librados.h rados_write_full"
    c_rados_write_full
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall unsafe "librados.h rados_append"
    c_rados_append
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall unsafe "librados.h rados_read"
    c_rados_read
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> CULLong
        -> IO CInt

foreign import ccall unsafe "librados.h rados_remove"
    c_rados_remove
        :: Ptr RadosIOCtxT
        -> CString
        -> IO CInt

foreign import ccall unsafe "librados.h rados_lock_exclusive"
    c_rados_lock_exclusive
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CString
        -> CString
        -> Ptr TimeVal
        -> LockFlag
        -> IO CInt

foreign import ccall unsafe "librados.h rados_unlock"
    c_rados_unlock
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CString
        -> IO CInt

foreign import ccall unsafe "librados.h rados_lock_shared"
    c_rados_lock_shared
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CString
        -> CString
        -> CString
        -> Ptr TimeVal
        -> LockFlag
        -> IO CInt
