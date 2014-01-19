{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
module System.Rados
(
    -- *Types
    Connection,
    Pool,
    Object,
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
    asyncReadChunk,
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
    withIdempotentExclusiveLock,
    withSharedLock,
    withIdempotentSharedLock,
    test
)
where

import Control.Exception (bracket, bracket_, throwIO)
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


newtype Connection a = Connection (ReaderT I.Connection IO a)
    deriving (Monad, MonadIO, MonadReader I.Connection)

newtype Pool a = Pool (ReaderT I.Pool (StateT Completions IO) a)
    deriving (Functor, Monad, MonadIO, MonadState Completions,  MonadReader I.Pool)

newtype Object a = Object (ReaderT B.ByteString Pool a)
    deriving (Functor, Monad, MonadIO, MonadReader B.ByteString)

data AsyncAction = ActionFailure E.RadosError | ActionInFlight I.Completion
data AsyncRead = ReadFailure E.RadosError | ReadInFlight I.Completion B.ByteString

type Completions = Map.Map I.Completion ()

askObjectPool :: Object (B.ByteString, I.Pool) 
askObjectPool = do
    liftM2 (,) ask (Object . lift $ ask)

writeChunk :: Word64 -> B.ByteString -> Object ()
writeChunk offset buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWrite pool object offset buffer

writeFull :: B.ByteString -> Object ()
writeFull buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncWriteFull pool object buffer

readChunk :: Word64 -> Word64 -> Object (Either E.RadosError B.ByteString)
readChunk len offset = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncRead pool object len offset

readFull :: Object (Either E.RadosError B.ByteString)
readFull = do
    result <- stat
    case result of
        Left e -> return $ Left e
        Right (size, _) -> readChunk size 0


append :: B.ByteString -> Object ()
append buffer = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncAppend pool object buffer

stat :: Object (Either E.RadosError (Word64, EpochTime))
stat = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncStat pool object

remove :: Object ()
remove = do
    (object, pool) <- askObjectPool
    liftIO $ I.syncRemove pool object

asyncWriteChunk :: Word64 -> B.ByteString -> Object AsyncAction
asyncWriteChunk offset buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWrite pool completion object offset buffer

asyncWriteFull :: B.ByteString -> Object AsyncAction
asyncWriteFull buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncWriteFull pool completion object buffer

asyncReadChunk :: Word64 -> Word64 -> Object AsyncRead
asyncReadChunk len offset = do
    (object, pool) <- askObjectPool
    withReadCompletion $ \completion -> 
        liftIO $ I.asyncRead pool completion object len offset

asyncAppend :: B.ByteString -> Object AsyncAction
asyncAppend buffer = do
    (object, pool) <- askObjectPool
    withActionCompletion $ \completion -> 
        liftIO $ I.asyncAppend pool completion object buffer

asyncRemove :: Object AsyncAction
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
            e <- liftIO $ do
                I.waitForSafe completion
                I.getAsyncError completion 
            modifyCompletions (\cs -> Map.delete completion cs)
            return $ either Just (const Nothing) e

look :: (MonadIO m, CompletionsState m)
     => AsyncRead -> m (Either E.RadosError B.ByteString)
look async_request =
    case async_request of
        ReadFailure e ->
            return $ Left e
        ReadInFlight completion bs -> do
            e <- liftIO $ do
                I.waitForSafe completion
                I.getAsyncError completion 
            deleteCompletion completion
            return $ either Left (const $ Right bs) e

class CompletionsState m where
    modifyCompletions :: (Completions -> Completions) -> m ()
    deleteCompletion :: I.Completion -> m ()
    deleteCompletion c = modifyCompletions (\cs -> Map.delete c cs)

instance CompletionsState Pool where
    modifyCompletions = modify

instance CompletionsState Object where
    modifyCompletions f = Object . lift $ modify f
    
-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withActionCompletion :: (I.Completion -> IO (Either E.RadosError a)) -> Object (AsyncAction)
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
withReadCompletion :: (I.Completion -> IO (Either E.RadosError B.ByteString)) -> Object (AsyncRead)
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
    runConnect Nothing (parseConfig "ceph.conf") $ do
        runPool "pool" $ do
            s <- runObject "hai" stat
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
-- runConnect (parseConfig \"ceph.conf\") $ do
--     ...
-- @
runConnect
    :: Maybe B.ByteString
    -> (I.Connection -> IO (Maybe E.RadosError))
    -> Connection a -- user action
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
    (result, completions) <- liftIO $ bracket
        (I.newPool connection pool)
        I.cleanupPool
        (\conn -> runStateT (runReaderT action conn) Map.empty)
    -- Maybe the user didn't care about the result of some of the async
    -- requests they made, so we clean up after them
    liftIO $ mapM_ I.cleanupCompletion (Map.keys completions)
    return result

runObject :: B.ByteString -> Object a -> Pool a
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
