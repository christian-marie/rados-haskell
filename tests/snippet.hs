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
{-# OPTIONS -fno-warn-unused-imports #-}

module Main where

import System.Rados

main :: IO ()
main = do
    withConnection Nothing (readConfig "/etc/ceph/ceph.conf") $ \connection ->
        withPool connection "test1" $ \pool ->
            runAsync allSafe $ do
                asyncWriteFull pool "tests/snippet.hs" "schrodinger's hai?\n"
                asyncWrite pool "tests/snippet.hs" 14 "cat"
