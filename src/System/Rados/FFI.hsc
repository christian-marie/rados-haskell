{-# LANGUAGE EmptyDataDecls           #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE CPP #-}
-- |
-- Module      : System.Rados.FFI
-- Copyright   : (c) 2010-2014 Anchor
-- License     : BSD-3
-- Maintainer  : Christian Marie <christian@ponies.io>
-- Stability   : experimental
-- Portability : non-portable
--
-- The underlying FFI wrappers, feel free to use these. I will not remove any
-- between major versions and they shouldn't need to change.

module System.Rados.FFI
where

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

-- typedef void *rados_list_ctx_t;
data RadosListCtxT

-- typedef void *rados_completion_t;
data RadosCompletionT

-- typedef void *rados_write_op_t;
data RadosWriteOpT

type RadosCallback  = Ptr RadosCompletionT -> Ptr () -> IO ()
type RadosCallbackT = FunPtr RadosCallback

newtype LockFlag = LockFlag { unLockFlag :: Word8 }
#{enum LockFlag, LockFlag, idempotent = LIBRADOS_LOCK_FLAG_RENEW }

data TimeVal = TimeVal
    { seconds      :: CLong 
    , microseconds :: CLong
    } deriving (Eq, Show)

instance Num TimeVal where
    (+) = undefined
    (*) = undefined
    (-) = undefined
    abs = undefined
    signum = undefined
    fromInteger i = let clong = fromIntegral i in TimeVal clong 0

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

foreign import ccall safe "librados.h rados_create"
    c_rados_create :: Ptr (Ptr RadosT)
                   -> CString
                   -> IO CInt

foreign import ccall safe "librados.h rados_shutdown"
    c_rados_shutdown :: Ptr RadosT
                     -> IO ()

foreign import ccall safe "librados.h rados_conf_read_file"
    c_rados_conf_read_file :: Ptr RadosT
                            -> CString
                            -> IO CInt

foreign import ccall safe "librados.h rados_conf_parse_argv"
    c_rados_conf_parse_argv :: Ptr RadosT
                            -> CInt
                            -> Ptr CString
                            -> IO CInt

foreign import ccall safe "librados.h rados_conf_parse_env"
    c_rados_conf_parse_env :: Ptr RadosT
                           -> CString
                           -> IO CInt

foreign import ccall unsafe "librados.h rados_connect"
    c_rados_connect :: Ptr RadosT
                    -> IO CInt

foreign import ccall safe "librados.h rados_ioctx_create"
    c_rados_ioctx_create
        :: Ptr RadosT
        -> CString
        -> Ptr (Ptr RadosIOCtxT)
        -> IO CInt

foreign import ccall safe "librados.h rados_ioctx_destroy"
    c_rados_ioctx_destroy :: Ptr RadosIOCtxT -> IO ()

foreign import ccall safe "librados.h rados_aio_create_completion"
    c_rados_aio_create_completion
        :: Ptr ()
        -> RadosCallbackT
        -> RadosCallbackT
        -> Ptr (Ptr RadosCompletionT)
        -> IO CInt

foreign import ccall safe "librados.h &rados_aio_release"
    c_rados_aio_release :: FunPtr (Ptr RadosCompletionT -> IO ())

foreign import ccall safe "string.h strerror"
    c_strerror :: Errno -> IO (Ptr CChar)

foreign import ccall safe "librados.h rados_aio_wait_for_complete"
    c_rados_aio_wait_for_complete :: Ptr RadosCompletionT -> IO CInt

foreign import ccall safe "librados.h rados_aio_wait_for_safe"
    c_rados_aio_wait_for_safe :: Ptr RadosCompletionT -> IO CInt

foreign import ccall safe "librados.h rados_aio_get_return_value"
    c_rados_aio_get_return_value :: Ptr RadosCompletionT -> IO CInt

foreign import ccall safe "librados.h rados_aio_read"
    c_rados_aio_read
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> Word64
        -> IO CInt

foreign import ccall safe "librados.h rados_aio_write"
    c_rados_aio_write
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> Word64
        -> IO CInt


foreign import ccall safe "librados.h rados_aio_write_full"
    c_rados_aio_write_full
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> IO CInt


foreign import ccall safe "librados.h rados_aio_append"
    c_rados_aio_append
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall safe "librados.h rados_aio_stat"
    c_rados_aio_stat
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> Ptr Word64
        -> Ptr CTime
        -> IO CInt

foreign import ccall safe "librados.h rados_aio_remove"
    c_rados_aio_remove
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr RadosCompletionT
        -> IO CInt

foreign import ccall safe "librados.h rados_write"
    c_rados_write :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> Word64
        -> IO CInt

foreign import ccall safe "librados.h rados_write_full"
    c_rados_write_full
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall safe "librados.h rados_append"
    c_rados_append
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> IO CInt

foreign import ccall safe "librados.h rados_read"
    c_rados_read
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CSize
        -> Word64
        -> IO CInt

foreign import ccall safe "librados.h rados_remove"
    c_rados_remove
        :: Ptr RadosIOCtxT
        -> CString
        -> IO CInt

foreign import ccall safe "librados.h rados_stat"
    c_rados_stat
        :: Ptr RadosIOCtxT
        -> CString
        -> Ptr Word64
        -> Ptr CTime
        -> IO CInt

foreign import ccall safe "librados.h rados_lock_exclusive"
    c_rados_lock_exclusive
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CString
        -> CString
        -> Ptr TimeVal
        -> LockFlag
        -> IO CInt

foreign import ccall safe "librados.h rados_unlock"
    c_rados_unlock
        :: Ptr RadosIOCtxT
        -> CString
        -> CString
        -> CString
        -> IO CInt

foreign import ccall safe "librados.h rados_lock_shared"
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

foreign import ccall safe "librados.h rados_nobjects_list_open"
    c_rados_nobjects_list_open
        :: Ptr RadosIOCtxT
        -> Ptr (Ptr RadosListCtxT)
        -> IO CInt

foreign import ccall unsafe "librados.h rados_nobjects_list_close"
    c_rados_nobjects_list_close
        :: Ptr RadosListCtxT
        -> IO ()

foreign import ccall safe "librados.h rados_nobjects_list_next"
    c_rados_nobjects_list_next
        :: Ptr RadosListCtxT
        -> Ptr CString
        -> Ptr CString
        -> Ptr CString
        -> IO CInt

foreign import ccall safe "getProgArgv"
    c_getProgArgv
        :: Ptr CInt
        -> Ptr (Ptr CString)
        -> IO ()

#if defined(ATOMIC_WRITES)
newtype ComparisonFlag = ComparisonFlag { unComparisonFlag :: Word8 }
#{enum ComparisonFlag, ComparisonFlag,
    nop = LIBRADOS_CMPXATTR_OP_NOP,
    eq  = LIBRADOS_CMPXATTR_OP_EQ,
    ne  = LIBRADOS_CMPXATTR_OP_NE,
    gt  = LIBRADOS_CMPXATTR_OP_GT,
    gte = LIBRADOS_CMPXATTR_OP_GTE,
    lt  = LIBRADOS_CMPXATTR_OP_LT,
    lte = LIBRADOS_CMPXATTR_OP_LTE
}

foreign import ccall safe "librados.h rados_create_write_op"
    c_rados_create_write_op
        :: IO (Ptr RadosWriteOpT)

foreign import ccall safe "librados.h &rados_release_write_op"
    c_rados_release_write_op
        :: FunPtr (Ptr RadosWriteOpT -> IO ())

foreign import ccall safe "librados.h rados_write_op_assert_exists"
    c_rados_write_op_assert_exists
        :: Ptr RadosWriteOpT
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_cmpxattr"
    c_rados_write_op_cmpxattr
        :: Ptr RadosWriteOpT
        -> CString
        -> ComparisonFlag
        -> CString
        -> CSize
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_setxattr"
    c_rados_write_op_setxattr
        :: Ptr RadosWriteOpT
        -> CString
        -> CString
        -> CSize
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_rmxattr"
    c_rados_write_op_rmxattr
        :: Ptr RadosWriteOpT
        -> CString
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_create"
    c_rados_write_op_create
        :: Ptr RadosWriteOpT
        -> CInt
        -> CString
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_write"
    c_rados_write_op_write
        :: Ptr RadosWriteOpT
        -> CString
        -> CSize
        -> Word64
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_write_full"
    c_rados_write_op_write_full
        :: Ptr RadosWriteOpT
        -> CString
        -> CSize
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_append"
    c_rados_write_op_append
        :: Ptr RadosWriteOpT
        -> CString
        -> CSize
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_remove"
    c_rados_write_op_remove
        :: Ptr RadosWriteOpT
        -> IO ()

foreign import ccall safe "librados.h rados_write_op_operate"
    c_rados_write_op_operate
        :: Ptr RadosWriteOpT
        -> Ptr RadosIOCtxT
        -> CString
        -> Ptr CTime
        -> IO (CInt)

foreign import ccall safe "librados.h rados_aio_write_op_operate"
    c_rados_aio_write_op_operate
        :: Ptr RadosWriteOpT
        -> Ptr RadosIOCtxT
        -> Ptr RadosCompletionT
        -> CString
        -> Ptr CTime
        -> IO (CInt)
#endif
