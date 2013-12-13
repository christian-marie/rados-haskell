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

{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module TestSuite where

import Test.Hspec
import Test.HUnit

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy as L
import Debug.Trace

--
-- What we're actually testing.
--

import System.Rados

suite :: Spec
suite = do
    describe "Connectivity" $ do
        testConnectionHost

    describe "Simple write/read round trips" $ do
        testPutObject
        testGetObject


testConnectionHost = do
    it "able to establish connetion to local Ceph cluster" $ 
        pendingWith "needs a test evaluating Connection only"


testPutObject =
    it "write object accepted by storage cluster" $ do
        withConnection Nothing (readConfig "/etc/ceph/ceph.conf") $ \connection ->
            withPool connection "test1" (\pool -> do
                syncWriteFull pool "test-rados-haskell" "schrodinger's hai?\n"
                syncWrite pool "test-rados-haskell" 14 "cat"
                assertBool "Failed" True)

testGetObject =
    it "read object returns correct data" $ do
        withConnection Nothing (readConfig "/etc/ceph/ceph.conf") (\connection ->
            withPool connection "test1" $ \pool -> do
                x' <- syncRead pool "test-rados-haskell" 0 1024
                assertEqual "Incorrect content read" "schrodinger's cat?\n" x')
