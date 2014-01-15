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

module TestSuite where

import Test.Hspec
import Test.HUnit

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Control.Monad
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
        testDeleteObject
    
    describe "Locking" $ do
        testLockWithoutOID

withTestPool :: (Pool -> IO a) -> IO a
withTestPool f = 
        withConnection Nothing (readConfig "/etc/ceph/ceph.conf") $ \connection ->
            withPool connection "test1" f

testConnectionHost, testPutObject, testGetObject, testDeleteObject :: Spec
testLockWithoutOID :: Spec
testConnectionHost =
    it "able to establish connetion to local Ceph cluster" $
        withTestPool $ (\pool -> return $ pool `seq` ()) 


testPutObject =
    it "write object accepted by storage cluster" $
        withTestPool $ \pool -> do
                syncWriteFull pool "test/TestSuite.hs" "schrodinger's hai?\n"
                syncWrite pool "test/TestSuite.hs" 14 "cat"
                assertBool "Failed" True

testGetObject =
    it "read object returns correct data" $
        withTestPool $ \pool -> do
            x' <- syncRead pool "test/TestSuite.hs" 0 1024
            assertEqual "Incorrect content read" "schrodinger's cat?\n" x'

testDeleteObject =
    it "deletes the object afterward" $
        withTestPool $ \pool -> do
            syncRemove pool "test/TestSuite.hs"
            assertBool "Failed" True

testLockWithoutOID =
    it "locks and unlocks quickly" $
        withTestPool $ \pool -> do
            replicateM_ 100 $ 
                withExclusiveLock pool "locked" "name" "desc" (Just 1) $
                    assertBool "Failed" True
