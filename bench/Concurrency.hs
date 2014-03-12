{-# LANGUAGE OverloadedStrings #-}

module Main where

import System.Rados
import Control.Applicative
import Control.Monad.IO.Class
import Control.Exception
import Criterion.Main
import qualified Control.Concurrent.Async as A
import Control.Concurrent.MVar
import Data.ByteString (ByteString)
import Control.Monad
import qualified Data.ByteString.Char8 as B

randomWords :: IO [ByteString]
randomWords = B.lines <$> B.readFile "/usr/share/dict/words"

forAsyncs :: (AsyncWrite -> Async (Maybe RadosError)) -> [ByteString] -> Int -> IO ()
forAsyncs complete_action oids n_concurrent = do
    runConnect Nothing (parseConfig "/etc/ceph/cloud.conf") $
        runPool "bench" . runAsync $ do
            runTest testAppend
            runTest testRemove
  where
    testAppend o = runObject o $ append "four"
    testRemove o = runObject o remove
    check f a    = f a >>= maybe (return ()) (liftIO . throwIO)

    runTest action = do
        work_mvar <- liftIO $ newMVar oids
        threads <- replicateM n_concurrent $
            async $ doWork action work_mvar
        liftIO $ mapM_ A.wait threads

    doWork action work_mvar = do
        work <- liftIO $ takeMVar work_mvar
        if null work then
            liftIO $ putMVar work_mvar work
        else do
            liftIO $ putMVar work_mvar $ tail work
            completion <- action $ head work
            check complete_action completion
            doWork action work_mvar

main :: IO ()
main = do
    oids <- take 10000 <$> randomWords
    defaultMain 
        [ bgroup "waitComplete"
            [ bench "1 concurrent" $ nfIO $ forAsyncs waitComplete oids 1
            , bench "2 concurrent" $ nfIO $ forAsyncs waitComplete oids 2
            , bench "4 concurrent" $ nfIO $ forAsyncs waitComplete oids 4
            , bench "8 concurrent" $ nfIO $ forAsyncs waitComplete oids 8
            , bench "16 concurrent" $ nfIO $ forAsyncs waitComplete oids 16
            , bench "32 concurrent" $ nfIO $ forAsyncs waitComplete oids 32
            , bench "64 concurrent" $ nfIO $ forAsyncs waitComplete oids 64
            ]
        ]
