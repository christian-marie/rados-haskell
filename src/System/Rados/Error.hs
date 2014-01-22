{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RecordWildCards    #-}
module System.Rados.Error
(
    RadosError(..),
    checkError,
    checkError',
    checkError_,
    maybeError,
    checkErrorRetryBusy_,
) where

import Control.Exception
import Control.Monad.Error
import Data.Typeable
import Foreign.C.Error
import Foreign.C.String
import Foreign.C.Types
import System.Rados.FFI as F

-- | An error indicated by librados, usually in the form of a negative return
-- value
data RadosError = Unknown  { errno     :: Int    -- ^ Error number (positive)
                           , cFunction :: String -- ^ The underlying C function
                           , strerror  :: String -- ^ The \"nice\" error message.
                           }
                -- | Usually returned if a file does not exist
                | NoEntity { errno     :: Int    
                           , cFunction :: String 
                           , strerror  :: String
                           }
                -- | Returned if a file already exists, and should not.
                | Exists   { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                -- | Returned in the event of a failed atomic transaction
                | Canceled { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                -- | A value was out of range, returned when reading or writing
                -- from/to invalid regions.
                | Range    { errno     :: Int
                           , cFunction :: String
                           , strerror  :: String
                           }
                | User     { message :: String }
    deriving (Eq, Ord, Typeable)


instance Error RadosError where
    strMsg err = User err

instance Show RadosError where
    show Unknown{..} = "rados: unknown rados error in '" ++
        cFunction ++ "', errno " ++ show errno ++ ": '" ++ strerror ++ "'"
    show NoEntity{..} = cFunction ++ ": ENOENT: '" ++ strerror ++ "'"
    show Exists{..} = cFunction ++ ": EEXIST: '" ++ strerror ++ "'"
    show Canceled{..} = cFunction ++ ": ECANCELED: '" ++ strerror ++ "'"
    show Range{..} = cFunction ++ ": ERANGE: '" ++ strerror ++ "'"

instance Exception RadosError

-- Handle a ceph Errno, which is an errno that must be negated before being
-- passed to strerror. Otherwise, treat the result a positive int and pass it
-- straight through.
--
-- This is needed for a few methods like rados_read that throw an error or
-- return the bytes read via the same CInt.
checkError :: String -> IO CInt -> IO Int
checkError function action = do
    checkError' function action >>= either throwIO return

checkError' :: String -> IO CInt -> IO (Either RadosError Int)
checkError' function action = do
    n <- action
    if n < 0
        then do
            let errno = (-n)
            strerror <- peekCString =<< F.c_strerror (Errno errno)
            return . Left $ makeError (fromIntegral errno) function strerror
        else return . Right $ fromIntegral n

maybeError :: String -> IO CInt -> IO (Maybe RadosError)
maybeError function action =
    checkError' function action >>= either (return . Just) (const $ return Nothing)

makeError :: Int -> String -> String -> RadosError
makeError 125 fun str = Canceled 125 fun str
makeError 2 fun str   = NoEntity 2 fun str
makeError 17 fun str  = Exists 17 fun str
makeError 34 fun str  = Range 34 fun str
makeError n fun str   = Unknown n fun str

checkError_ :: String -> IO CInt -> IO ()
checkError_ function action = void $ checkError function action

-- Retry if EBUSY
checkErrorRetryBusy_ :: String -> IO CInt -> IO ()
checkErrorRetryBusy_ function action = do
    result <- checkError' function action
    case result of
        Left rados_error ->
            if errno rados_error == 16
            then checkErrorRetryBusy_ function action
            else throwIO rados_error
        Right _ -> return ()
