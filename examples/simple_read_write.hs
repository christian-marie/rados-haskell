{-# LANGUAGE OverloadedStrings #-}
module Main where
import System.Rados
import Control.Exception
import qualified Data.ByteString.Char8 as B

main :: IO ()
main = do
    kitty <- runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "magic_pool" . runObject "an oid" $ do
            writeFull "hello kitty!"
            readFull
    either throwIO B.putStrLn kitty
