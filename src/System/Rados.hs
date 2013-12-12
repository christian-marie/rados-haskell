module System.Rados
(
    I.ClusterHandle,
    I.Completion,
    I.IOContext,
    withConnection,
    withContext,
    readConfig,
)
where

import qualified System.Rados.Internal as I
import qualified Data.ByteString as B
import Control.Exception (bracket)

-- |
-- Run an action with a 'ClusterHandle' to ceph, cleanup is handled via 'bracket'.
--
-- First argument is an action that configures the handle prior to connection.
--
-- @
-- withConnection (readConfig "ceph.conf") $ \connection -> do
--     ...
-- @
withConnection :: Maybe B.ByteString
                  -> (I.ClusterHandle -> IO ()) -- configure action
                  -> (I.ClusterHandle -> IO a) -- user action
                  -> IO a
withConnection user configure action =
    bracket
        (do h <- I.newClusterHandle user
            configure h
            I.connect h
            return h)
        I.cleanupClusterHandle
        action

-- |
-- Open a 'IOContext' with ceph and perform an action with it, cleaning up with
-- 'bracket'.
--
-- @
-- ...
--     withContext connection "pool42" $ \pool ->
--         ...
-- @
withContext :: I.ClusterHandle -> B.ByteString -> (I.IOContext -> IO a) -> IO a
withContext connection pool action =
    bracket
        (I.newIOContext connection pool)
        I.cleanupIOContext
        action

--- |
-- Read a config from a relative or absolute 'FilePath' into a 'ClusterHandle'.
--
-- Intended for use with 'withConnection'.
readConfig :: FilePath -> I.ClusterHandle -> IO ()
readConfig = flip I.confReadFile
