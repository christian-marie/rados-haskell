{-# LANGUAGE OverloadedStrings #-}
module Main where
import System.Rados.Base
import Control.Exception
import qualified Data.ByteString.Char8 as B

main :: IO ()
main = do
    kitty <- withConnection Nothing $ \c -> do
        confReadFile c "/etc/ceph/ceph.conf"
        connect c
        withIOContext c "test" $ \ctx -> do
            Nothing <- syncWriteFull ctx "an oid" "hello kitty!"
            syncRead ctx "an oid" 12 0
    either throwIO B.putStrLn kitty
