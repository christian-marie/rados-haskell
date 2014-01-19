{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
module System.Rados
(
    -- *Types
    -- **Monads
    Connection,
    Pool,
    Object,
    -- **Data types
    StatResult,
    fileSize,
    modifyTime,
    -- *Exceptions
    -- |
    -- This library should only ever throw a 'RadosError'.
    --
    -- You can handle this with something like:
    --
    -- @
    -- main = tryHai \`catch\` (\\e -> putStrLn $ strerror e )
    --   where tryHai = runConnect Nothing (parseConfig \"/dev/null\")
    --                                         (\\_ -> putStrLn \"hai\")
    -- @
    E.RadosError(..),
    -- *General usage
    -- |
    -- There are a series of convenience monads to avoid you having to carry
    -- around state yourself:
    --
    -- @
    -- writeRead :: IO ByteString
    -- writeRead =
    --     runConnect Nothing (parseConfig \"ceph.conf\") $
    --         runPool connection \"magic_pool\" $
    --             runObject "oid" $
    --                 writeFull \"hai!\"
    -- @
    runConnect,
    runPool,
    runObject,
    -- *Syncronous IO
    writeChunk,
    writeFull,
    readChunk,
    readFull,
    append,
    stat,
    remove,
    -- *Asynchronous IO
    -- ** Async functions
    asyncWriteChunk,
    asyncWriteFull,
    asyncAppend,
    asyncRemove,
    -- ** Completion functions
    waitSafe,
    look,
    -- *Configuration
    parseConfig,
    parseArgv,
    parseEnv,
    -- *Locking
    withExclusiveLock,
    withSharedLock,
)
where

import Control.Exception (bracket, bracket_, throwIO)
import Control.Monad.State
import Control.Monad.Reader
import Control.Applicative
import Data.Word (Word64)
import System.Posix.Types(EpochTime)
import Foreign.ForeignPtr
import Foreign.Storable
import System.IO.Unsafe

import qualified Data.ByteString.Char8 as B
import qualified System.Rados.Error as E
import qualified System.Rados.Internal as I
import Data.UUID
import Data.UUID.V4

newtype Connection a = Connection (ReaderT I.Connection IO a)
    deriving (Monad, MonadIO, MonadReader I.Connection)

newtype Pool a = Pool (ReaderT I.Pool IO a)
    deriving (Functor, Monad, MonadIO, MonadReader I.Pool)

newtype Object parent a = Object (ReaderT B.ByteString parent a)
    deriving (Functor, Monad, MonadIO, MonadReader B.ByteString)

newtype Asynchronous a = Asynchronous (ReaderT I.Pool IO a)
    deriving (Functor, Monad, MonadIO, MonadReader I.Pool)

data AsyncAction = ActionFailure E.RadosError | ActionInFlight I.Completion
data AsyncRead a = ReadFailure E.RadosError | ReadInFlight I.Completion a
data StatResult = StatResult Word64 EpochTime
          | StatInFlight (ForeignPtr Word64) (ForeignPtr EpochTime)

fileSize :: StatResult -> Word64
fileSize (StatResult s _) = s
fileSize (StatInFlight s _) = unsafePerformIO $ withForeignPtr s peek
modifyTime :: StatResult -> EpochTime
modifyTime (StatResult _ m) = m
modifyTime (StatInFlight _ m) = unsafePerformIO $ withForeignPtr m peek

class Monad m => RadosReader m wrapper | m -> wrapper where
    readChunk :: Word64 -> Word64 -> m (wrapper B.ByteString)
    readFull :: m (wrapper B.ByteString)
    readFull = do
        s <- stat >>= unWrap
        case s of
            Left e -> wrapFail e
            Right r -> readChunk (fileSize r) 0
    stat :: m (wrapper StatResult)
    unWrap :: wrapper a -> m (Either E.RadosError a)
    wrapFail :: E.RadosError -> m (wrapper a)

instance RadosReader (Object Pool) (Either E.RadosError) where
    readChunk len offset = do
        (object, pool) <- askObjectPool
        liftIO $ I.syncRead pool object len offset

    stat = do
        (object, pool) <- askObjectPool
        liftIO $ do
            s <- I.syncStat pool object
            return $ case s of
                Left e -> Left e
                Right (size, time) -> Right $ StatResult size time

    unWrap = return . id
    wrapFail = return . Left

instance RadosReader (Object Asynchronous) AsyncRead where
    readChunk len offset = do
        (object, pool) <- askObjectPool
        withReadCompletion $ \completion -> 
            liftIO $ I.asyncRead pool completion object len offset

    stat = do
        (object, pool) <- askObjectPool
        withReadCompletion $ \completion ->
            liftIO $ do
                s <- I.asyncStat pool completion object
                return $ case s of
                    Left e ->
                        Left e
                    Right (size_fp, mtime_fp) ->
                        Right $ StatInFlight size_fp mtime_fp
        
    unWrap = look
    wrapFail = return . ReadFailure

askObjectPool :: MonadReader I.Pool m => Object m (B.ByteString, I.Pool) 
askObjectPool = do
    liftM2 (,) ask (Object . lift $ ask)

writeChunk :: Word64 -> B.ByteString ->  Object Pool ()
writeChunk offset buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWrite pool object offset buffer

writeFull :: B.ByteString ->  Object Pool ()
writeFull buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWriteFull pool object buffer



append :: B.ByteString ->  Object Pool ()
append buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncAppend pool object buffer

remove ::  Object Pool ()
remove = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncRemove pool object

asyncWriteChunk :: Word64 -> B.ByteString -> Object Asynchronous AsyncAction
asyncWriteChunk offset buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWrite pool completion object offset buffer

asyncWriteFull :: B.ByteString -> Object Asynchronous AsyncAction
asyncWriteFull buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWriteFull pool completion object buffer

asyncAppend :: B.ByteString -> Object Asynchronous AsyncAction
asyncAppend buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncAppend pool completion object buffer

asyncRemove :: Object Asynchronous AsyncAction
asyncRemove = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncRemove pool completion object

waitSafe :: (MonadIO m)
            => AsyncAction -> m (Maybe E.RadosError)
waitSafe async_request =
    case async_request of
        ActionFailure e ->
            return $ Just e
        ActionInFlight completion -> do
            e <- liftIO $ do
                I.waitForSafe completion
                I.getAsyncError completion 
            return $ either Just (const Nothing) e

look :: (MonadIO m)
     => AsyncRead a -> m (Either E.RadosError a)
look async_request =
    case async_request of
        ReadFailure e ->
            return $ Left e
        ReadInFlight completion a -> do
            e <- liftIO $ do
                I.waitForSafe completion
                I.getAsyncError completion 
            return $ either Left (const $ Right a) e


-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withActionCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> Object Asynchronous AsyncAction
withActionCompletion f = do
    completion <- liftIO I.newCompletion
    result <- liftIO $ f completion
    case result of
        Left e -> return $ ActionFailure e
        Right _ -> do
            return $ ActionInFlight completion
    
-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withReadCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> Object Asynchronous (AsyncRead a)
withReadCompletion f = do
    completion <- liftIO I.newCompletion
    result <- liftIO $ f completion
    case result of
        Left e -> return $ ReadFailure e
        Right a -> do
            return $ ReadInFlight completion a

-- |
-- Run an action with a 'Connection' to ceph, cleanup is handled via 'bracket'.
--
-- First argument is an optional user to connect as.
--
-- Second argument is an action that configures the handle prior to connection.
--
-- Third argument is the action to run with the connection made.
--
-- @
-- runConnect (parseConfig \"ceph.conf\") $ do
--     ...
-- @
runConnect
    :: Maybe B.ByteString
    -> (I.Connection -> IO (Maybe E.RadosError))
    -> Connection a
    -> IO a
runConnect user configure (Connection action) = do
    bracket
        (do h <- I.newConnection user
            conf <- configure h
            case conf of
                Just e -> do
                    I.cleanupConnection h
                    throwIO e
                Nothing -> do
                    I.connect h
                    return h)
        I.cleanupConnection
        (runReaderT action)

-- |
-- Open a 'Pool' with ceph and perform an action with it, cleaning up with
-- 'bracket'.
--
-- @
-- ...
--     runPool \"pool42\" $ do
--         ...
-- @
runPool :: B.ByteString -> Pool a -> Connection a
runPool pool (Pool action) = do
    connection <- ask
    liftIO $ bracket
        (I.newPool connection pool)
        I.cleanupPool
        (\p -> runReaderT action p)

runObject :: B.ByteString -> Object m a -> m a
runObject object_id (Object action) = do
    runReaderT action object_id


--- |
-- Read a config from a relative or absolute 'FilePath' into a 'Connection'.
--
-- Intended for use with 'runConnect'.
parseConfig :: FilePath -> I.Connection -> IO (Maybe E.RadosError)
parseConfig = flip I.confReadFile

parseArgv :: I.Connection -> IO (Maybe E.RadosError)
parseArgv = I.confParseArgv

parseEnv :: I.Connection -> IO (Maybe E.RadosError)
parseEnv = I.confParseEnv

-- | Perform an action with an exclusive lock on oid
withExclusiveLock
    :: B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock
    -> Maybe I.TimeVal -- ^ Optional duration of lock
    -> Pool a          -- ^ Action to perform with lock
    -> Pool a
withExclusiveLock oid name desc duration action =
    withLock oid name action $ \pool cookie -> 
        I.exclusiveLock pool oid name cookie desc duration []

-- | Perform an action with an shared lock on oid and tag
withSharedLock
    :: B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock
    -> B.ByteString    -- ^ Tag for shared lock
    -> Maybe I.TimeVal -- ^ Optional duration of lock
    -> Pool a          -- ^ Action to perform with lock
    -> Pool a
withSharedLock oid name desc tag duration action =
    withLock oid name action $ \pool cookie -> 
        I.sharedLock pool oid name cookie tag desc duration []

withLock
    :: B.ByteString
    -> B.ByteString
    -> Pool a
    -> (I.Pool -> B.ByteString -> IO b)
    -> Pool a
withLock oid name (Pool user_action) lock_action = do
    pool <- ask
    cookie <- liftIO $ B.pack . toString <$> nextRandom
    -- Re-wrap user's action in a sub-ReaderT that is identical, this way we
    -- can just use bracket_ to ensure the lock is cleaned up even if they
    -- generate an exception.
    liftIO $ bracket_
        (lock_action pool cookie)
        (I.unlock pool oid name cookie)
        (runReaderT user_action pool)
