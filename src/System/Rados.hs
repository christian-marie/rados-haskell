{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}

module System.Rados
(
    -- |
    -- * General usage
    --
    -- There are a series of convenience monads to avoid you having to carry
    -- around state yourself:
    --
    -- @
    -- writeRead :: IO ByteString
    -- writeRead =
    --     runConnect Nothing (parseConfig \"ceph.conf\") $
    --         runPool connection \"magic_pool\" $
    --             runObject \"oid\" $
    --                 writeFull \"hai!\"
    -- @
    --
    -- A note on reading signatures:
    --
    -- #signatures#
    -- In order to have the same reading and writing API presented to you
    -- within 'Async', 'Pool' and 'Atomic' monads, the return value the basic
    -- read/write functions are not fixed.
    --
    -- The return value wrappers are documented in the 'runObject', 'runAsync'
    -- and 'runAtomicWrite' sections.
    -- * Initialization
    runConnect,
    parseConfig,
    parseArgv,
    parseEnv,
    runPool,
    -- * Asynchronous requests
    runAsync,
    waitSafe,
    look,
    runObject,
    runAtomicWrite,
    -- * Extra atomic operations
    assertExists,
    compareXAttribute,
    I.eq, I.ne, I.gt, I.gte, I.lt, I.lte, I.nop,
    setXAttribute,

    -- * Reading
    readChunk,
    readFull,
    stat,
    -- * Writing
    writeChunk,
    writeFull,
    append,
    remove,
    -- * Locking
    withExclusiveLock,
    withSharedLock,
    -- * Types
    -- ** Monads
    Connection,
    Pool,
    Object,
    Async,
    -- ** Data types
    StatResult,
    fileSize,
    modifyTime,
    -- *Exceptions
    -- |
    -- This library should never throw an error within runPool, runPool itself
    -- may throw a 'RadosError' should it have a problem opening the given
    -- pool. Any exception thrown within runPool will be thrown all the way out
    -- to 'IO'.
    E.RadosError(..),
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
import Data.Typeable
import Data.Maybe

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

newtype Async a = Async (ReaderT I.Pool IO a)
    deriving (Functor, Monad, MonadIO, MonadReader I.Pool)

newtype AtomicWrite a = AtomicWrite (ReaderT I.WriteOperation IO a)
    deriving (Functor, Monad, MonadIO, MonadReader I.WriteOperation)

data AsyncAction = ActionFailure E.RadosError | ActionInFlight I.Completion
data AsyncRead a = ReadFailure E.RadosError | ReadInFlight I.Completion a
data StatResult = StatResult Word64 EpochTime
                | StatInFlight (ForeignPtr Word64) (ForeignPtr EpochTime)
    deriving (Typeable)

-- Make reading a StatResult transparent

fileSize :: StatResult -> Word64
fileSize (StatResult s _) = s
fileSize (StatInFlight s _) = unsafePerformIO $ withForeignPtr s peek
modifyTime :: StatResult -> EpochTime
modifyTime (StatResult _ m) = m
modifyTime (StatInFlight _ m) = unsafePerformIO $ withForeignPtr m peek

class Monad m => RadosWriter m e | m -> e where
    writeChunk
        :: Word64          -- ^ Offset
        -> B.ByteString    -- ^ Data to write
        -> m e             -- ^ Possible error, see: "System.Rados#signatures"
    writeFull :: B.ByteString -> m e
    append :: B.ByteString -> m e
    remove :: m e

class Monad m => AtomicWriter m e | m -> e where
    -- | Must be run within an Object monad, this will run all writes
    -- atomically. The writes will be queued up, and on execution of the monad
    -- will be sent to ceph in one batch request.
    --
    -- @
    -- e <- runOurPool . runObject \"hi\" . runAtomicWrite $ do
    --     remove
    --     writeChunk 42 "fourty-two"
    -- isNothing e
    -- @
    --
    -- Or for async:
    --
    -- @
    -- e <- runOurPool . runAsync . runObject \"hi\" . runAtomicWrite $ do
    --     remove
    --     writeChunk 42 "fourty-two"
    -- isNothing <$> waitSafe e
    -- @
    runAtomicWrite :: AtomicWrite a -> m e

class Monad m => RadosReader m wrapper | m -> wrapper where
    readChunk :: Word64 -> Word64 -> m (wrapper B.ByteString)
    readFull :: m (wrapper B.ByteString)
    readFull =
        stat >>= unWrap >>= either wrapFail (\r -> readChunk  (fileSize r) 0)

    stat :: m (wrapper StatResult)
    unWrap :: Typeable a => wrapper a -> m (Either E.RadosError a)
    wrapFail :: E.RadosError -> m (wrapper a)

instance RadosWriter (Object Pool) (Maybe E.RadosError) where
    writeChunk offset buffer = do
        (object, pool) <- askObjectPool
        liftIO $ I.syncWrite pool object offset buffer

    writeFull buffer = do
        (object, pool) <- askObjectPool
        liftIO $ I.syncWriteFull pool object buffer

    append buffer = do
        (object, pool) <- askObjectPool
        liftIO $ I.syncAppend pool object buffer

    remove = do
        (object, pool) <- askObjectPool
        liftIO $ I.syncRemove pool object

instance RadosWriter (Object Async) AsyncAction where
    writeChunk offset buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ I.asyncWrite pool completion object offset buffer

    writeFull buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ I.asyncWriteFull pool completion object buffer

    append buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ I.asyncAppend pool completion object buffer

    remove = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ I.asyncRemove pool completion object

instance RadosWriter AtomicWrite () where
    writeChunk offset buffer = do
        op <- ask
        liftIO $ I.writeOperationWrite op buffer offset

    writeFull buffer = do
        op <- ask
        liftIO $ I.writeOperationWriteFull op buffer

    append buffer = do
        op <- ask
        liftIO $ I.writeOperationAppend op buffer

    remove = do
        op <- ask
        liftIO $ I.writeOperationRemove op

instance AtomicWriter (Object Pool) (Maybe E.RadosError) where
    runAtomicWrite (AtomicWrite action) = do
        (object, pool) <- askObjectPool
        liftIO $ do
            op <- I.newWriteOperation
            runReaderT action op
            I.writeOperate op pool object

instance AtomicWriter (Object Async) AsyncAction where
    runAtomicWrite (AtomicWrite action) = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion ->
            liftIO $ do
                op <- I.newWriteOperation
                runReaderT action op
                I.asyncWriteOperate op pool completion object

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

instance RadosReader (Object Async) AsyncRead where
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

look :: (MonadIO m, Typeable a)
     => AsyncRead a -> m (Either E.RadosError a)
look async_request =
    case async_request of
        ReadFailure e ->
            return $ Left e
        ReadInFlight completion a -> do
            ret <- liftIO $ do
                I.waitForSafe completion
                I.getAsyncError completion 
            return $ case ret of
                Left e -> Left e
                Right n -> Right $
                    -- This is a hack to trim async bytestrings to the correct
                    -- size on recieving the actual number of bytes read from
                    -- getAsyncError. The casting is needed so that the user
                    -- can simply use "look" to look at any read value, it just
                    -- so happens that when the user looks at a ByteString
                    -- value we magically trim it to the correct size.
                    case (cast a :: Maybe B.ByteString) of
                        Just bs -> fromJust . cast $ B.take n bs
                        Nothing -> a

-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withActionCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> Object Async AsyncAction
withActionCompletion f = do
    completion <- liftIO I.newCompletion
    result <- liftIO $ f completion
    case result of
        Left e -> return $ ActionFailure e
        Right _ -> do
            return $ ActionInFlight completion
    
-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withReadCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> Object Async (AsyncRead a)
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
-- This may throw a RadosError to IO if the pool cannot be opened.
--
-- For the following examples, we shall use:
--
-- @
-- runOurPool :: Pool a -> IO a
-- runOurPool = 
--     runConnect Nothing parseArgv . runPool "magic_pool"
-- @
runPool :: B.ByteString -> Pool a -> Connection a
runPool pool (Pool action) = do
    connection <- ask
    liftIO $ bracket
        (I.newPool connection pool)
        I.cleanupPool
        (\p -> runReaderT action p)

-- |
-- runObject is a convenience/readability monad to store the provide object id
-- and provide it to read and write actions.
-- @
-- runOurPool . runObject \"an oid\" readFull
-- @
runObject :: B.ByteString -> Object m a -> m a
runObject object_id (Object action) = do
    runReaderT action object_id

-- |
-- Any read/writes within this context will be run asynchronously, a
-- runAtomicWrite run within this monad will also run that atomic write
-- asynchronously.
--
-- Return values of reads and writes are wrapped within 'AsyncRead' or
-- 'AsyncAction' respectively. You may extract the actual value from a read via
-- 'look' and 'waitSafe'.
--
-- The asynchronous nature of execution here means that if you fail to inspect
-- asynchronous writes with 'waitSafe', you will never know if they failed.
--
-- @
-- runOurPool . runAsync $ runObject \"a box\" $ do
--   wr <- writeFull \"schrodinger's hai?\\n\"
--   writeChunk 14 \"cat\" -- Don't care about the cat.
--   print . isNothing \<$\> waitSafe wr
--   r <- readFull >>= look
--   either throwIO print r
-- @
runAsync :: Async a -> Pool a
runAsync (Async action) = do
    pool <- ask
    liftIO $ runReaderT action pool

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
    -> Maybe Double    -- ^ Optional duration of lock
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
    -> Maybe Double    -- ^ Optional duration of lock
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

assertExists :: AtomicWrite ()
assertExists = do
    op <- ask
    liftIO $ I.writeOperationAssertExists op

compareXAttribute :: B.ByteString -> I.ComparisonFlag -> B.ByteString -> AtomicWrite ()
compareXAttribute key operator value = do
    op <- ask
    liftIO $ I.writeOperationCompareXAttribute op key operator value

setXAttribute :: B.ByteString -> B.ByteString -> AtomicWrite ()
setXAttribute key value = do
    op <- ask
    liftIO $ I.writeOperationSetXAttribute op key value

