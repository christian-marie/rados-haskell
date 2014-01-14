--
-- Haskell bindings to librados
--
-- Copyright © 2013-     Anchor Systems, Pty Ltd
--
-- The code in this file, and the program it is a part of, is
-- made available to you by its authors as open source software:
-- you can redistribute it and/or modify it under the terms of
-- the BSD licence.
--

{-# LANGUAGE OverloadedStrings #-}

module Main where

import System.Rados
import Control.Monad
import Criterion.Main

withoutLocking :: IO ()
withoutLocking = do
    withConnection Nothing (readConfig "/etc/ceph/prod.conf") $ \connection ->
        withPool connection "vaultaire" $ \pool -> replicateM_ 10 $
            syncAppend pool "append benchmark" "HI!"
                

withLocking :: IO ()
withLocking = do
    withConnection Nothing (readConfig "/etc/ceph/prod.conf") $ \connection ->
        withPool connection "vaultaire" $ \pool -> replicateM_ 10 $ do
            withExclusiveLock pool "append benchmark" "lock name" "lock description" (Just 30) $ 
                syncAppend pool "append benchmark" "HI!"
                

main :: IO ()
main = do
    defaultMain 
        [ bench "10 appends without locking" $ nfIO withoutLocking
        , bench "10 appends with locking" $ nfIO withLocking
        ]
