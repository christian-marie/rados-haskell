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
    -- Write an object and then read it back:
    --
    -- @
    -- writeRead :: IO ByteString
    -- writeRead =
    --     withConnection Nothing (readConfig \"ceph.conf\") $ \\connection ->
    --         withPool connection \"magic_pool\" $ \\pool -> do
    --             syncWriteFull pool \"oid\" \"hai!\"
    --             syncRead pool \"oid\" 0 4
    -- @
    withConnection,
    withPool,
    -- *Syncronous IO
    I.syncRead,
    I.syncWrite,
    I.syncWriteFull,
    I.syncAppend,
    I.syncRemove,
    -- *Asynchronous IO
    -- ** Async monad
    runAsync,
    -- ** Completion functions
    allComplete,
    allSafe,
    -- ** Async functions
    asyncWrite,
    asyncWriteFull,
    asyncAppend,
    -- *Configuration
    readConfig,
    -- *Locking
    withExclusiveLock,
    withIdempotentExclusiveLock,
    withSharedLock,
    withIdempotentSharedLock,
)
where

import Control.Exception (bracket)
import Control.Monad.State
import Control.Applicative

import qualified Data.ByteString.Char8 as B
import Data.Either
import Data.Word
import qualified System.Rados.Error as E
import qualified System.Rados.Internal as I
import Data.UUID
import Data.UUID.V4

newtype Async a = Async (StateT [Either E.RadosError I.Completion] IO a)
    deriving (Monad, MonadIO, MonadState [Either E.RadosError I.Completion])

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
-- withConnection (readConfig \"ceph.conf\") $ \\connection -> do
--     ...
-- @
withConnection
    :: Maybe B.ByteString
    -> (I.Connection -> IO ()) -- configure action
    -> (I.Connection -> IO a) -- user action
    -> IO a
withConnection user configure=
    bracket
        (do h <- I.newConnection user
            configure h
            I.connect h
            return h)
        I.cleanupConnection

-- |
-- Open a 'Pool' with ceph and perform an action with it, cleaning up with
-- 'bracket'.
--
-- @
-- ...
--     withPool connection \"pool42\" $ \\pool ->
--         ...
-- @
withPool :: I.Connection -> B.ByteString -> (I.Pool -> IO a) -> IO a
withPool connection pool =
    bracket
        (I.newPool connection pool)
        I.cleanupPool

--- |
-- Read a config from a relative or absolute 'FilePath' into a 'Connection'.
--
-- Intended for use with 'withConnection'.
readConfig :: FilePath -> I.Connection -> IO ()
readConfig = flip I.confReadFile

-- |
-- Run some write actions asyncronously, then wait on all of these actions
-- using a completion function.
--
-- You may chose how to wait on the actions run within the Async monad
-- when you provide a completion function, this function will iterate over
-- the internal completions associated with each action and wait
-- accordingly.
--
-- runAsync will not return until the completion function has returned and it
-- has cleaned up all resources.
--
-- The return value is a list of 'Maybe' 'RadosError', corresponding to the
-- possible errors associated with actions in the order of which they are
-- executed within the monad.
--
-- @
-- ...
--         runAsync allSafe $ do
--             asyncWriteFull pool \"oid2\" \"moar hai!\"
--             asyncWriteFull pool \"oid3\" \"simultaneous hais!\"
--         putStrLn \"oid2 and oid3 were written to stable storage\"
-- ...
-- @
runAsync :: ([I.Completion] -> IO ()) -> Async a -> IO [Maybe E.RadosError]
runAsync check (Async a) = do
    (_, results) <- runStateT a []
    errors <- forM results $ either (return . Just) I.getAsyncError
    check $ rights results
    mapM_ I.cleanupCompletion $ rights results
    return errors

-- |
-- The same as 'syncWrite', but does not block.
asyncWrite :: I.Pool -> B.ByteString -> Word64 -> B.ByteString -> Async ()
asyncWrite pool oid offset buffer =
    withCompletion $ \completion ->
        I.asyncWrite pool completion oid offset buffer
-- |
-- The same as 'syncWriteFull', but does not block.
asyncWriteFull :: I.Pool -> B.ByteString -> B.ByteString -> Async ()
asyncWriteFull pool oid buffer =
    withCompletion $ \completion ->
        I.asyncWriteFull pool completion oid buffer

-- |
-- The same as 'syncWriteAppend', but does not block.
asyncAppend :: I.Pool -> B.ByteString -> B.ByteString -> Async ()
asyncAppend pool oid buffer =
    withCompletion $ \completion ->
        I.asyncAppend pool completion oid buffer


-- | Run an action with a completion, cleaning up on failure, stashing in
-- state otherwise.
withCompletion :: (I.Completion -> IO (Either E.RadosError Int)) -> Async ()
withCompletion f = do
    completion <- liftIO I.newCompletion
    result     <- liftIO $ f completion
    case result of
        -- Our async action either fails to launch at all, in which case we
        -- have an error right now.
        Left e -> do
            liftIO $ I.cleanupCompletion completion
            modify (\xs -> Left e:xs )
        -- Or, it can fail later. In which case we will check it when the
        -- monad chain is evaluated.
        Right _ ->
            modify (\xs -> Right completion:xs)

-- |
-- All actions are in memory on all replicas.
allComplete :: [I.Completion] -> IO ()
allComplete = mapM_ I.waitForComplete

-- |
-- All actions are in stable storage on all replicas.
allSafe :: [I.Completion] -> IO ()
allSafe = mapM_ I.waitForSafe

-- | Perform an action with an exclusive lock on oid
withExclusiveLock, withIdempotentExclusiveLock
    :: I.Pool
    -> B.ByteString    -- ^ oid
    -> B.ByteString    -- ^ name
    -> B.ByteString    -- ^ desc
    -> Maybe I.TimeVal -- ^ duration
    -> IO a
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
    -> B.ByteString    -- ^ oid
    -> B.ByteString    -- ^ name
    -> B.ByteString    -- ^ desc
    -> B.ByteString    -- ^ tag
    -> Maybe I.TimeVal -- ^ duration
    -> IO a
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
    bracket
        (do cookie <- B.pack . toString <$> nextRandom
            lock_action cookie
            return cookie)
        (I.unlock pool oid name)
        (\_ -> user_action) -- Doesn't need the cookie


