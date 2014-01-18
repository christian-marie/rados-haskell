{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
module System.Rados
(
    -- *Types
    I.Connection,
    I.Pool,
    I.TimeVal(..),
    -- *Exceptions
    -- |
    -- This library should only ever throw a 'RadosError'.
    --
    -- You can handle this with something like:
    --
    -- @
    -- main = tryHai \`catch\` (\\e -> putStrLn $ strerror e )
    --   where tryHai = withConnection Nothing (readConfig \"/dev/null\")
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
    --     withConnection Nothing (readConfig \"ceph.conf\") $
    --         withPool connection \"magic_pool\" $
    --             withObject "oid" $
    --                 writeFull \"hai!\"
    -- @
    withConnection,
    withPool,
    withObject,
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
    asyncReadChunk,
    asyncAppend,
    asyncRemove,
    -- ** Completion functions
    waitSafe,
    look,
    -- *Configuration
    readConfig,
    -- *Locking
    withExclusiveLock,
    withIdempotentExclusiveLock,
    withSharedLock,
    withIdempotentSharedLock,
    test
)
where

import Control.Exception (bracket, bracket_)
import Control.Monad.State
import Control.Monad.Reader
import Control.Applicative
import qualified Data.Map as Map
import Data.Word (Word64)
import System.Posix.Types(EpochTime)

import qualified Data.ByteString.Char8 as B
import qualified System.Rados.Error as E
import qualified System.Rados.Internal as I
import Data.UUID
import Data.UUID.V4


newtype ConnectionReader a = ConnectionReader (ReaderT I.Connection IO a)
    deriving (Monad, MonadIO, MonadReader I.Connection)

newtype PoolStateReader a = PoolStateReader (ReaderT I.Pool (StateT Completions IO) a)
    deriving (Functor, Monad, MonadIO, MonadState Completions,  MonadReader I.Pool)

newtype ObjectReader a = ObjectReader (ReaderT B.ByteString PoolStateReader a)
    deriving (Functor, Monad, MonadIO, MonadReader B.ByteString)

data AsyncAction = ActionFailure E.RadosError | ActionInFlight I.Completion
data AsyncRead = ReadFailure E.RadosError | ReadInFlight I.Completion B.ByteString

type Completions = Map.Map I.Completion ()

askObjectPool :: ObjectReader (B.ByteString, I.Pool) 
askObjectPool = do
    liftM2 (,) ask (ObjectReader . lift $ ask)

writeChunk :: Word64 -> B.ByteString -> ObjectReader ()
writeChunk offset buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWrite pool object offset buffer

writeFull :: B.ByteString -> ObjectReader ()
writeFull buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWriteFull pool object buffer

readChunk :: Word64 -> Word64 -> ObjectReader (Either E.RadosError B.ByteString)
readChunk len offset = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncRead pool object len offset

readFull :: ObjectReader (Either E.RadosError B.ByteString)
readFull = do
    result <- stat
    case result of
        Left e -> return $ Left e
        Right (size, _) -> readChunk size 0


append :: B.ByteString -> ObjectReader ()
append buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncAppend pool object buffer

stat :: ObjectReader (Either E.RadosError (Word64, EpochTime))
stat = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncStat pool object

remove :: ObjectReader ()
remove = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncRemove pool object

asyncWriteChunk :: Word64 -> B.ByteString -> ObjectReader AsyncAction
asyncWriteChunk offset buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWrite pool completion object offset buffer

asyncWriteFull :: B.ByteString -> ObjectReader AsyncAction
asyncWriteFull buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWriteFull pool completion object buffer

asyncReadChunk :: Word64 -> Word64 -> ObjectReader AsyncRead
asyncReadChunk len offset = do
    (object, pool) <- askObjectPool
    withReadCompletion $ \completion -> 
        liftIO $ I.asyncRead pool completion object len offset

asyncAppend :: B.ByteString -> ObjectReader AsyncAction
asyncAppend buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncAppend pool completion object buffer

asyncRemove :: ObjectReader AsyncAction
asyncRemove = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncRemove pool completion object

waitSafe :: (MonadIO m, CompletionsState m)
            => AsyncAction -> m (Maybe E.RadosError)
waitSafe async_request =
    case async_request of
        ActionFailure e ->
            return $ Just e
        ActionInFlight completion -> do
            e <- liftIO $ I.getAsyncError completion 
            liftIO $ I.cleanupCompletion completion
            modifyCompletions (\cs -> Map.delete completion cs)
            return $ either Just (const Nothing) e

look :: (MonadIO m, CompletionsState m)
     => AsyncRead -> m (Either E.RadosError B.ByteString)
look async_request =
    case async_request of
        ReadFailure e ->
            return $ Left e
        ReadInFlight completion bs -> do
            e <- liftIO $ I.getAsyncError completion 
            liftIO $ I.cleanupCompletion completion
            deleteCompletion completion
            return $ either Left (const $ Right bs) e

class CompletionsState m where
    modifyCompletions :: (Completions -> Completions) -> m ()
    deleteCompletion :: I.Completion -> m ()
    deleteCompletion c = modifyCompletions (\cs -> Map.delete c cs)

instance CompletionsState PoolStateReader where
    modifyCompletions = modify

instance CompletionsState ObjectReader where
    modifyCompletions f = ObjectReader . lift $ modify f
    
-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withActionCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> ObjectReader (AsyncAction)
withActionCompletion f = do
    completion <- liftIO I.newCompletion
    result <- liftIO $ f completion
    case result of
        Left e -> return $ ActionFailure e
        Right _ -> do
            modifyCompletions (\cs -> Map.insert completion () cs)
            return $ ActionInFlight completion
    
-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withReadCompletion :: (I.Completion -> IO (Either E.RadosError B.ByteString)) -> ObjectReader (AsyncRead)
withReadCompletion f = do
    completion <- liftIO I.newCompletion
    result <- liftIO $ f completion
    case result of
        Left e -> return $ ReadFailure e
        Right bs -> do
            modifyCompletions (\cs -> Map.insert completion () cs)
            return $ ReadInFlight completion bs

test :: IO ()
test = do
    withConnection Nothing (readConfig "ceph.conf") $ do
        withPool "pool" $ do
            s <- withObject "hai" stat
            liftIO $ print s

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
-- withConnection (readConfig \"ceph.conf\") $ do
--     ...
-- @
withConnection
    :: Maybe B.ByteString
    -> ConnectionReader () -- configure action
    -> ConnectionReader a -- user action
    -> IO a
withConnection user (ConnectionReader configure) (ConnectionReader action) = do
    bracket
        (do h <- I.newConnection user
            I.connect h
            return h)
        I.cleanupConnection
        (runReaderT (configure >> action))

-- |
-- Open a 'Pool' with ceph and perform an action with it, cleaning up with
-- 'bracket'.
--
-- @
-- ...
--     withPool \"pool42\" $ do
--         ...
-- @
withPool :: B.ByteString -> PoolStateReader a -> ConnectionReader a
withPool pool (PoolStateReader action) = do
    connection <- ask
    (result, completions) <- liftIO $ bracket
        (I.newPool connection pool)
        I.cleanupPool
        (\conn -> runStateT (runReaderT action conn) Map.empty)
    -- Maybe the user didn't care about the result of some of the async
    -- requests they made, so we clean up after them
    liftIO $ mapM_ I.cleanupCompletion (Map.keys completions)
    return result

withObject :: B.ByteString -> ObjectReader a -> PoolStateReader a
withObject object_id (ObjectReader action) = do
    runReaderT action object_id


--- |
-- Read a config from a relative or absolute 'FilePath' into a 'Connection'.
--
-- Intended for use with 'withConnection'.
readConfig :: FilePath -> ConnectionReader ()
readConfig path = do
    connection <- ask
    liftIO $ I.confReadFile connection path

-- | Perform an action with an exclusive lock on oid
withExclusiveLock, withIdempotentExclusiveLock
    :: I.Pool
    -> B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock
    -> Maybe I.TimeVal -- ^ Optional duration of lock
    -> IO a            -- ^ Action to perform with lock
    -> IO a
withExclusiveLock pool oid name desc duration action =
    withLock pool oid name action $ \cookie -> 
        I.exclusiveLock pool oid name cookie desc duration []

withIdempotentExclusiveLock pool oid name desc duration action =
    withLock pool oid name action $ \cookie -> 
        I.exclusiveLock pool oid name cookie desc duration [I.idempotent]

-- | Perform an action with an shared lock on oid and tag
withSharedLock, withIdempotentSharedLock
    :: I.Pool
    -> B.ByteString    -- ^ Object ID
    -> B.ByteString    -- ^ Name of lock
    -> B.ByteString    -- ^ Description of lock
    -> B.ByteString    -- ^ Tag for shared lock
    -> Maybe I.TimeVal -- ^ Optional duration of lock
    -> IO a            -- ^ Action to perform with lock
    -> IO a
withSharedLock pool oid name desc tag duration action =
    withLock pool oid name action $ \cookie -> 
        I.sharedLock pool oid name cookie tag desc duration []

withIdempotentSharedLock pool oid name desc tag duration action =
    withLock pool oid name action $ \cookie ->
        I.sharedLock pool oid name cookie tag desc duration [I.idempotent]

withLock
    :: I.Pool
    -> B.ByteString
    -> B.ByteString
    -> IO b
    -> (B.ByteString -> IO a)
    -> IO b
withLock pool oid name user_action lock_action = do
    cookie <- B.pack . toString <$> nextRandom
    bracket_
        (lock_action cookie)
        (I.unlock pool oid name cookie)
        user_action
