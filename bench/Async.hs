{-# LANGUAGE OverloadedStrings #-}

module Main where

import System.Rados
import Control.Applicative
import Control.Monad.IO.Class
import Control.Exception
import Criterion.Main
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B

randomWords :: IO [ByteString]
randomWords = B.lines <$> B.readFile "/usr/share/dict/words"

waitAllSafe, waitAllComplete :: [ByteString] -> IO ()
waitAllSafe     = forAsyncs waitSafe
waitAllComplete = forAsyncs waitComplete

forAsyncs :: (AsyncWrite -> Async (Maybe RadosError)) -> [ByteString] -> IO ()
forAsyncs f oids = do
    runConnect (Just "pingu") (parseConfig "/etc/ceph/prod.conf") $
        runPool "vaultaire" . runAsync $ do
            mapM testAppend oids >>= cleanup
            mapM testRemove oids >>= cleanup
            return ()
  where
    testAppend o = runObject o $ append "hai"
    testRemove o = runObject o remove
    cleanup as   = mapM check as
    check a      = f a >>= maybe (return ()) (liftIO . throwIO)
            
main :: IO ()
main = do
    oids <- take 1000 <$> randomWords
    defaultMain 
        [ bench "waitSafe" $ nfIO $ waitAllSafe oids
        , bench "waitComplete " $ nfIO $ waitAllComplete oids
        ]
