{-# LANGUAGE OverloadedStrings #-}
module Main where
import System.Rados.Monadic
import Control.Exception
import qualified Data.ByteString.Char8 as B

main :: IO ()
main = do
    kitty <- runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
        runPool "test" . runObject "an oid" $ do
            Nothing <- writeFull "hello kitty!"
            readFull
    either throwIO B.putStrLn kitty
