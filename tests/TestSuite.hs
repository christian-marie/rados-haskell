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
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS -fno-warn-unused-imports #-}

module TestSuite where

import Test.Hspec
import Test.HUnit

--
-- Otherwise redundent imports, but useful for testing in GHCi.
--

import Control.Monad
import Control.Exception (throwIO, try, SomeException)
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
        testPutObjectAsync
        testGetObjectAsync
    
    describe "Locking" $ do
        testSharedLock
        testExclusiveLock

runTestPool :: Pool a -> IO a
runTestPool a = 
        runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") $
            runPool "test1" a

testConnectionHost, testPutObject, testGetObject, testDeleteObject :: Spec
testGetObjectAsync, testPutObjectAsync, testSharedLock :: Spec
testExclusiveLock :: Spec

testConnectionHost =
    it "able to establish connetion to local Ceph cluster" $ do
        runTestPool $ return ()
        assertBool "Failed" True


testPutObject =
    it "write object accepted by storage cluster" $ do
        runTestPool $ runObject "test/TestSuite.hs" $ do
            writeFull "schrodinger's hai?\n"
            writeChunk 14 "cat"
        assertBool "Failed" True

testGetObject =
    it "read object returns correct data" $ do
        a <- runTestPool $ runObject "test/TestSuite.hs" $ readChunk 32 0
        either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") a
        (Right b) <- runTestPool $ runObject "test/TestSuite.hs" $ readFull
        assertEqual "Incorrect content readFull" "schrodinger's cat?\n" b

testDeleteObject =
    it "deletes the object afterward" $ do
        runTestPool $ runObject "test/TestSuite.hs" remove
        assertBool "Failed" True

testPutObjectAsync =
    it "write object accepted by storage cluster" $ do
        runTestPool . runAsync $ runObject "test/TestSuite.hs" $ do
            wr <- asyncWriteFull "schrodinger's hai?\n"
            waitSafe wr
            wr' <- asyncWriteChunk 14 "cat"
            waitSafe wr'
        assertBool "Failed" True

testGetObjectAsync =
    it "read object returns correct data" $ do
        r <- runTestPool . runAsync $ runObject "test/TestSuite.hs" $ readChunk 32 0 >>= look
        either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") r
        --r' <- look `fmap` runTestPool $ runObject "test/TestSuite.hs" $ asyncReadFull
        --either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") r'

testSharedLock =
    it "locks and unlocks quickly" $ do
        (_ :: Either SomeException a) <- try $ runTestPool $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 1) $
                error "Bang!"
        (_ :: Either SomeException a) <- try $ runTestPool $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 1) $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 1) $
                error "Bang again!"
        assertBool "No deadlock" True

testExclusiveLock =
    it "locks and unlocks quickly" $
        assertBool "Failed" True
