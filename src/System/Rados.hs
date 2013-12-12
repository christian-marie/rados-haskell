module System.Rados
(
    -- *Types
    I.Connection,
    I.Completion,
    I.Pool,
    -- *General usage
    withConnection,
    withContext,
    -- *Syncronous IO
    I.syncRead,
    I.syncWrite,
    I.syncWriteFull,
    I.syncAppend,
    -- *Configuration
    readConfig,
)
where

import qualified System.Rados.Internal as I
import qualified Data.ByteString as B
import Control.Exception (bracket)

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
withConnection :: Maybe B.ByteString
                  -> (I.Connection -> IO ()) -- configure action
                  -> (I.Connection -> IO a) -- user action
                  -> IO a
withConnection user configure action =
    bracket
        (do h <- I.newConnection user
            configure h
            I.connect h
            return h)
        I.cleanupConnection
        action

-- |
-- Open a 'Pool' with ceph and perform an action with it, cleaning up with
-- 'bracket'.
--
-- @
-- ...
--     withContext connection \"pool42\" $ \\pool ->
--         ...
-- @
withContext :: I.Connection -> B.ByteString -> (I.Pool -> IO a) -> IO a
withContext connection pool action =
    bracket
        (I.newPool connection pool)
        I.cleanupPool
        action

--- |
-- Read a config from a relative or absolute 'FilePath' into a 'Connection'.
--
-- Intended for use with 'withConnection'.
readConfig :: FilePath -> I.Connection -> IO ()
readConfig = flip I.confReadFile


