{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ConstraintKinds #-}

-- |
-- Module      : System.Rados.Monadic
-- Copyright   : (c) 2010-2014 Anchor
-- License     : BSD-3
-- Maintainer  : Christian Marie <christian@ponies.io>
-- Stability   : experimental
-- Portability : non-portable
--
-- Monadic interface to librados, covers async read/writes, locks and atomic
-- writes (ensure you use the build flag).
--
-- This is the monadic API, you may use the underlying internals or FFI calls
-- via "System.Rados.Base" and "System.Rados.FFI".
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
--     either throwIO B.putStrLn (kitty :: Either RadosError B.ByteString)
-- @
--
module System.Rados.Monadic
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
    --     a2 \<- runAsync . runObject \"object\" $ readFull
    --     a3 \<- look a2
    --     a1 :: Either RadosError ByteString
    --     a2 :: AsyncRead ByteString
    --     a3 :: Either RadosError ByteString
    -- @

    -- ** Reading API
    RadosReader(readChunk, readFull, stat),
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
    RadosWriter(..),
    -- * Asynchronous requests
    async,
    runAsync,
    waitSafe,
    waitComplete,
    look,
    runObject,
#if defined(ATOMIC_WRITES)
    runAtomicWrite,
    -- * Extra atomic operations
    assertExists,
    compareXAttribute,
    B.eq, B.ne, B.gt, B.gte, B.lt, B.lte, B.nop,
    setXAttribute,
#endif
    -- * Locking
    withExclusiveLock,
    withSharedLock,
    -- * Types
    -- ** Data types
    StatResult,
    fileSize,
    modifyTime,
    AsyncRead,
    AsyncWrite,
    -- ** Monads
    Connection,
    Pool,
    Object,
    Async,
    -- ** Exceptions
    -- |
    -- This library should never throw an error within runPool, runPool itself
    -- may throw a 'RadosError' should it have a problem opening the given
    -- pool.
    E.RadosError(..),
    -- ** Re-exports
    liftIO
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
import qualified Control.Concurrent.Async as A
import qualified Data.ByteString.Char8 as B
import qualified System.Rados.Error as E
import qualified System.Rados.Base as B
import Data.UUID
import Data.UUID.V4

newtype Connection a = Connection (ReaderT B.Connection IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader B.Connection)

newtype Pool a = Pool (ReaderT B.IOContext IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader B.IOContext)

newtype Object parent a = Object (ReaderT B.ByteString parent a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader B.ByteString)

newtype Async a = Async (ReaderT B.IOContext IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader B.IOContext)

#if defined(ATOMIC_WRITES)
newtype AtomicWrite a = AtomicWrite (ReaderT B.WriteOperation IO a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader B.WriteOperation)
#endif

-- | A write request in flight, access a possible error with 'waitSafe'
data AsyncWrite = ActionFailure E.RadosError | ActionInFlight B.Completion
-- | A read request in flight, access the contents of the read with 'look'
data AsyncRead a = ReadFailure E.RadosError | ReadInFlight B.Completion a
-- | The result of a 'stat', access the contents with 'modifyTime' and
-- 'fileSize'
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

class (MonadReader B.IOContext m, MonadIO m) => PoolReader m where
    unPoolReader :: m a -> ReaderT B.IOContext IO a

class Monad m => RadosWriter m e | m -> e where
    -- | Write a chunk of data
    --
    -- The possible types of this function are:
    --
    -- @
    -- writeChunk :: Word64 -> ByteString -> Object Pool (Maybe RadosError)
    -- writeChunk :: Word64 -> ByteString -> Object A.AsyncWrite
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
    -- writeFull :: ByteString -> Object A.AsyncWrite
    -- @
    writeFull :: B.ByteString -> m e

    -- | Append to the end of an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- append :: ByteString -> Object Pool (Maybe RadosError)
    -- append :: ByteString -> Object A.AsyncWrite
    -- @
    append :: B.ByteString -> m e

    -- | Delete an object
    --
    -- The possible types of this function are:
    --
    -- @
    -- remove :: Object Pool (Maybe RadosError)
    -- remove :: Object A.AsyncWrite
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

instance PoolReader Async where
    unPoolReader (Async a) = a

instance PoolReader Pool where
    unPoolReader (Pool a) = a

instance RadosWriter (Object Pool) (Maybe E.RadosError) where
    writeChunk offset buffer = do
        (object, pool) <- askObjectPool
        liftIO $ B.syncWrite pool object offset buffer

    writeFull buffer = do
        (object, pool) <- askObjectPool
        liftIO $ B.syncWriteFull pool object buffer

    append buffer = do
        (object, pool) <- askObjectPool
        liftIO $ B.syncAppend pool object buffer

    remove = do
        (object, pool) <- askObjectPool
        liftIO $ B.syncRemove pool object

instance RadosWriter (Object Async) AsyncWrite where
    writeChunk offset buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ B.asyncWrite pool completion object offset buffer

    writeFull buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ B.asyncWriteFull pool completion object buffer

    append buffer = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ B.asyncAppend pool completion object buffer

    remove = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion -> 
            liftIO $ B.asyncRemove pool completion object

#if defined(ATOMIC_WRITES)
instance RadosWriter AtomicWrite () where
    writeChunk offset buffer = do
        op <- ask
        liftIO $ B.writeOperationWrite op buffer offset

    writeFull buffer = do
        op <- ask
        liftIO $ B.writeOperationWriteFull op buffer

    append buffer = do
        op <- ask
        liftIO $ B.writeOperationAppend op buffer

    remove = do
        op <- ask
        liftIO $ B.writeOperationRemove op

instance AtomicWriter (Object Pool) (Maybe E.RadosError) where
    runAtomicWrite (AtomicWrite action) = do
        (object, pool) <- askObjectPool
        liftIO $ do
            op <- B.newWriteOperation
            runReaderT action op
            B.writeOperate op pool object

instance AtomicWriter (Object Async) AsyncWrite where
    runAtomicWrite (AtomicWrite action) = do
        (object, pool) <- askObjectPool
        withActionCompletion $ \completion ->
            liftIO $ do
                op <- B.newWriteOperation
                runReaderT action op
                B.asyncWriteOperate op pool completion object
#endif

instance RadosReader (Object Pool) (Either E.RadosError) where
    readChunk len offset = do
        (object, pool) <- askObjectPool
        liftIO $ B.syncRead pool object len offset

    stat = do
        (object, pool) <- askObjectPool
        liftIO $ do
            s <- B.syncStat pool object
            return $ case s of
                Left e -> Left e
                Right (size, time) -> Right $ StatResult size time

    unWrap = return . id
    wrapFail = return . Left

instance RadosReader (Object Async) AsyncRead where
    readChunk len offset = do
        (object, pool) <- askObjectPool
        withReadCompletion $ \completion -> 
            liftIO $ B.asyncRead pool completion object len offset

    stat = do
        (object, pool) <- askObjectPool
        withReadCompletion $ \completion ->
            liftIO $ do
                s <- B.asyncStat pool completion object
                return $ case s of
                    Left e ->
                        Left e
                    Right (size_fp, mtime_fp) ->
                        Right $ StatInFlight size_fp mtime_fp
        
    unWrap = look
    wrapFail = return . ReadFailure

askObjectPool :: MonadReader B.IOContext m => Object m (B.ByteString, B.IOContext) 
askObjectPool =
    liftM2 (,) ask (Object . lift $ ask)

-- | Wrapper for the Control.Concurrent.Async library, you must be very careful
-- to wait for the completion of all created async actions within the pool
-- monad, or they will run with an invalid (cleaned up) context.
--
-- This will be rectified in future versions when reference counting is
-- implemented, for now it is very unpolished and will require you to import
-- qualified Control.Concurrent.Async.
async :: PoolReader m => m a -> m (A.Async a) 
async action = do
    pool <- ask
    -- Stick the pool within async
    liftIO . A.async $ runReaderT (unPoolReader action) pool
    -- TODO: Implement reference counting here and within runPool

-- | Wait until a Rados write has hit stable storage on all replicas, you will
-- only know if a write has been successful when you inspect the AsyncWrite
-- with waitSafe.
--
-- Provides a Maybe RadosError.
--
-- @
-- runOurPool . runAsync . runObject \"a box\" $ do
--   async_request \<- writeFull \"schrodinger's hai?\\n\"
--   liftIO $ putStrLn "Write is in flight!"
--   maybe_error <- waitSafe async_request
--   case maybe_error of
--      Just e  -> liftIO $ print e
--      Nothing -> return ()
-- @
waitSafe :: MonadIO m => AsyncWrite -> m (Maybe E.RadosError)
waitSafe = waitAsync B.waitForSafe

-- | Wait until a Rados write has hit memory on all replicas. This is less safe
-- than waitSafe, but still pretty safe. Safe.
waitComplete :: MonadIO m => AsyncWrite -> m (Maybe E.RadosError)
waitComplete = waitAsync B.waitForComplete

waitAsync :: MonadIO m
          => (B.Completion -> IO a) -> AsyncWrite -> m (Maybe E.RadosError)
waitAsync f async_request =
    case async_request of
        ActionFailure e ->
            return $ Just e
        ActionInFlight completion -> do
            e <- liftIO $ do
                f completion
                B.getAsyncError completion 
            return $ either Just (const Nothing) e


-- | Take an 'AsyncRead' a and provide Either RadosError a
-- This function is used for retrieving the value of an async read.
--
-- @
-- runOurPool . runAsync . runObject \"a box\" $ do
--   async_read \<- readFull
--   liftIO $ putStrLn "Request is in flight!"
--   either_error_or_read \<- look async_read
--   either (liftIO . throwIO) BS.putStrLn  either_error_or_read
-- @
look :: (MonadIO m, Typeable a)
     => AsyncRead a -> m (Either E.RadosError a)
look async_request =
    case async_request of
        ReadFailure e ->
            return $ Left e
        ReadInFlight completion a -> do
            ret <- liftIO $ do
                B.waitForSafe completion
                B.getAsyncError completion 
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

-- | Run an action with a completion.
withActionCompletion :: (B.Completion -> IO (Either E.RadosError a)) -> Object Async AsyncWrite
withActionCompletion f = do
    completion <- liftIO B.newCompletion
    result <- liftIO $ f completion
    return $ case result of
        Left e  -> ActionFailure e
        Right _ -> ActionInFlight completion
    
-- | Run an read with a completion
withReadCompletion :: (B.Completion -> IO (Either E.RadosError a)) -> Object Async (AsyncRead a)
withReadCompletion f = do
    completion <- liftIO B.newCompletion
    result <- liftIO $ f completion
    return $ case result of
        Left e -> ReadFailure e
        Right a -> ReadInFlight completion a

-- |
-- Run an action within the 'Connection' monad, this may throw a RadosError to
-- IO if the connection or configuration fails.
--
-- @
-- runConnect (parseConfig \"ceph.conf\") $ runPool ...
-- @
runConnect
    :: Maybe B.ByteString                        -- ^ Optional user name
    -> (B.Connection -> IO (Maybe E.RadosError)) -- ^ Configuration function
    -> Connection a
    -> IO a
runConnect user configure (Connection action) =
    bracket
        (do h <- B.newConnection user
            conf <- configure h
            case conf of
                Just e -> do
                    B.cleanupConnection h
                    throwIO e
                Nothing -> do
                    B.connect h
                    return h)
        B.cleanupConnection
        (runReaderT action)

-- |
--
-- Run an action within the 'Pool' monad.
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
        (B.newIOContext connection pool)
        B.cleanupIOContext
        (runReaderT action)


-- |
-- Run an action within the 'Object m' monad, where m is the caller's context.
--
-- @
-- (runOurPool . runObject \"an oid\" :: Object Pool a -> IO a
-- (runOurPool . runAsync . runObject \"an oid\") :: Object Async a -> IO a
-- @
runObject :: PoolReader m =>
             B.ByteString -> Object m a -> m a
runObject object_id (Object action) =
    runReaderT action object_id

-- |
-- Any read/writes within this monad will be run asynchronously.
--
-- Return values of reads and writes are wrapped within 'AsyncRead' or
-- 'AsyncWrite' respectively. You should extract the actual value from a read
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
runAsync :: PoolReader m => Async a -> m a
runAsync (Async action) = do
    -- We merely re-wrap the pool.
    pool <- ask
    liftIO $ runReaderT action pool

-- | Read a config from a relative or absolute 'FilePath' into a 'Connection'.
--
-- Intended for use with 'runConnect'.
parseConfig :: FilePath -> B.Connection -> IO (Maybe E.RadosError)
parseConfig = flip B.confReadFile

-- | Read a config from the command line, note that no help flag will be
-- provided.
parseArgv :: B.Connection -> IO (Maybe E.RadosError)
parseArgv = B.confParseArgv

-- | Parse the contents of the environment variable CEPH_ARGS as if they were
-- ceph command line options.
parseEnv :: B.Connection -> IO (Maybe E.RadosError)
parseEnv = B.confParseEnv

-- | Perform an action with an exclusive lock.
withExclusiveLock
    :: B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock (debugging)
    -> Maybe Double    -- ^ Optional duration of lock
    -> Pool a          -- ^ Action to perform with lock
    -> Pool a
withExclusiveLock oid name desc duration action =
    withLock oid name action $ \pool cookie -> 
        B.exclusiveLock pool oid name cookie desc duration []

-- | Perform an action with an shared lock.
withSharedLock
    :: B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock (debugging)
    -> B.ByteString    -- ^ Tag for lock holder (debugging)
    -> Maybe Double    -- ^ Optional duration of lock
    -> Pool a          -- ^ Action to perform with lock
    -> Pool a
withSharedLock oid name desc tag duration action =
    withLock oid name action $ \pool cookie -> 
        B.sharedLock pool oid name cookie tag desc duration []

withLock
    :: B.ByteString
    -> B.ByteString
    -> Pool a
    -> (B.IOContext -> B.ByteString -> IO b)
    -> Pool a
withLock oid name (Pool user_action) lock_action = do
    pool <- ask
    cookie <- liftIO $ B.pack . toString <$> nextRandom
    -- Re-wrap user's action in a sub-ReaderT that is identical, this way we
    -- can just use bracket_ to ensure the lock is cleaned up even if they
    -- generate an exception.
    liftIO $ bracket_
        (lock_action pool cookie)
        (tryUnlock pool oid name cookie)
        (runReaderT user_action pool)
  where
    -- Handle the case of a lock possibly expiring. It's okay not to be able to
    -- remove a lock that does not exist.
    tryUnlock pool oid' name' cookie = do
        me <- B.unlock pool oid' name' cookie
        case me of
            Nothing -> return ()
            Just (E.NoEntity {}) -> return ()
            Just e -> throwIO e



#if defined(ATOMIC_WRITES)
assertExists :: AtomicWrite ()
assertExists = do
    op <- ask
    liftIO $ B.writeOperationAssertExists op

compareXAttribute :: B.ByteString -> B.ComparisonFlag -> B.ByteString -> AtomicWrite ()
compareXAttribute key operator value = do
    op <- ask
    liftIO $ B.writeOperationCompareXAttribute op key operator value

setXAttribute :: B.ByteString -> B.ByteString -> AtomicWrite ()
setXAttribute key value = do
    op <- ask
    liftIO $ B.writeOperationSetXAttribute op key value
#endif
