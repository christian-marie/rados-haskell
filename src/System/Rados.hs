{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}

-- |
-- Module      : System.Rados
-- Copyright   : (c) 2010-2014 Anchor
-- License     : BSD-3
-- Maintainer  : Christian Marie <christian@ponies.io>
-- Stability   : experimental
-- Portability : non-portable
--
-- librados haskell binding, covers asynchronous read/writes, locks and atomic
-- writes.
--
-- This is the monadic API, you may use the underlying internals or FFI calls
-- via 'System.Rados.Internal' and 'System.Rados.FFI'
--
-- A simple complete example:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
-- module Main where
-- import System.Rados
-- import Control.Exception
-- import qualified Data.ByteString.Char8 as B
-- main :: IO ()
-- main = do
--     kitty \<- runConnect Nothing (parseConfig \"ceph.conf\") $
--         runPool \"magic_pool\" . runObject \"an oid\" $ do
--             writeFull \"hello kitty!\"
--             readFull
--     either throwIO B.putStrLn kitty
-- @
--
module System.Rados
(
    -- * Initialization
    runConnect,
    parseConfig,
    parseArgv,
    parseEnv,
    runPool,
    -- * Reading
    -- ** A note on signatures
    --
    -- |In order to use these functions in any RadosReader monad, the type
    -- signatures have been made generic.
    --
    -- This allows the same API to be used for synchronous and asynchronous
    -- requests.
    --
    -- Thus, a1 and a2 below have different signatures:
    --
    -- @
    -- runOurPool $ do
    --     a1 \<- runObject \"object\" $ readFull
    --     a2 \<- runAsync . runObject \"object\" $ readChunk 42 6
    --     a3 \<- look a2
    --     a1 :: Either RadosError ByteString
    --     a2 :: AsyncRead ByteString
    --     a3 :: Either RadosError ByteString
    -- @

    -- ** Reading API
    readChunk,
    readFull,
    stat,
    -- * Writing
    -- ** A note on signatures
    --
    -- |In order to use these functions in any RadosWriter monad, the type
    -- signatures have been made generic.
    --
    -- This allows the same API to be used for synchronous and asynchronous
    -- requests.
    --
    -- Thus, a1 and a2 below have different signatures:
    --
    -- @
    -- runOurPool $ do
    --     a1 \<- runObject \"object\" $ writeFull \"hai!\"
    --     a2 \<- runAsync . runObject \"object\" $ writeFull \"hai!\"
    --     a3 \<- waitSafe a2
    --     a1 :: Maybe RadosError
    --     a2 :: AsyncWrite
    --     a3 :: Maybe RadosError
    -- @

    -- ** Writing API
    writeChunk,
    writeFull,
    append,
    remove,
    -- * Asynchronous requests
    runAsync,
    waitSafe,
    look,
    runObject,
#if defined(ATOMIC_WRITES)
    runAtomicWrite,
    -- * Extra atomic operations
    assertExists,
    compareXAttribute,
    I.eq, I.ne, I.gt, I.gte, I.lt, I.lte, I.nop,
    setXAttribute,
#endif
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

#if defined(ATOMIC_WRITES)
newtype AtomicWrite a = AtomicWrite (ReaderT I.WriteOperation IO a)
    deriving (Functor, Monad, MonadIO, MonadReader I.WriteOperation)
#endif

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
    -- | Write a chunk of data
    --
    -- The possible types of this function are:
    --
    -- @
    -- writeChunk :: Word64 -> ByteString -> Object Pool (Maybe RadosError)
    -- writeChunk :: Word64 -> ByteString -> Object Async AsyncAction
    -- @
    writeChunk
        :: Word64          -- ^ Offset to write at
        -> B.ByteString    -- ^ Bytes to write
        -> m e

    -- | Atomically replace an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- writeFull :: ByteString -> Object Pool (Maybe RadosError)
    -- writeFull :: ByteString -> Object Async AsyncAction
    -- @
    writeFull :: B.ByteString -> m e

    -- | Append to the end of an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- append :: ByteString -> Object Pool (Maybe RadosError)
    -- append :: ByteString -> Object Async AsyncAction
    -- @
    append :: B.ByteString -> m e

    -- | Delete an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- remove :: Object Pool (Maybe RadosError)
    -- remove :: Object Async AsyncAction
    -- @
    remove :: m e

#if defined(ATOMIC_WRITES)
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
#endif

class Monad m => RadosReader m wrapper | m -> wrapper where
    -- | Read a chunk of data.
    --
    -- The possible types of this function are:
    --
    -- @
    -- readChunk :: Word64 -> Word64 -> Object Pool (Either RadosError ByteString)
    -- readChunk :: Word64 -> Word64 -> Object Async (AsyncRead ByteString)
    -- @
    readChunk
        :: Word64 -- ^ Number of bytes to read
        -> Word64 -- ^ Offset to read from
        -> m (wrapper B.ByteString)

    -- | Read all avaliable data.
    --
    -- This is implemented with a stat followed by a read.
    --
    -- If you call this within the Object Async monad, the async request will
    -- wait for the result of the stat. The read itself will still be
    -- asynchronous.
    --
    -- The possible types of this function are:
    --
    -- @
    -- readFull :: Object Pool (Either RadosError ByteString)
    -- readFull :: Object Async (AsyncRead ByteString)
    -- @
    readFull :: m (wrapper B.ByteString)
    readFull =
        stat >>= unWrap >>= either wrapFail (\r -> readChunk  (fileSize r) 0)

    -- | Retrive the file size and mtime of an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- stat :: Object Pool (Either RadosError StatResult)
    -- stat :: Object Async (AsyncRead StatResult)
    -- @
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

#if defined(ATOMIC_WRITES)
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
#endif

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

waitSafe :: (RadosWriter m e, MonadIO m)
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

look :: (RadosReader m e, MonadIO m, Typeable a)
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
--     runConnect Nothing parseArgv . runPool \"magic_pool\"
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
-- Any read/writes within this monad will be run asynchronously.
--
-- Return values of reads and writes are wrapped within 'AsyncRead' or
-- 'AsyncAction' respectively. You should extract the actual value from a read
-- via 'look' and 'waitSafe'.
--
-- The asynchronous nature of error handling means that if you fail to inspect
-- asynchronous writes with 'waitSafe', you will never know if they failed.
--
-- @
-- runOurPool . runAsync . runObject \"a box\" $ do
--   wr <- writeFull \"schrodinger's hai?\\n\"
--   writeChunk 14 \"cat\" -- Don't care about the cat.
--   print . isNothing \<$\> waitSafe wr
--   r \<- readFull \>>= look
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

#if defined(ATOMIC_WRITES)
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
#endif
