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

{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module TestSuite where

import Test.Hspec
import Test.HUnit

import Control.Applicative
import Control.Monad
import qualified Control.Concurrent.Async as Async
import Control.Concurrent
import Control.Exception (SomeException, throwIO, try)
import Data.Maybe
import System.Rados.Monadic

suite :: Spec
suite = do
    describe "Thread safeness" $ do
        testConnectBug

    describe "Connectivity" $ do
        testConnectionHost


    describe "Simple write/read round trips" $ do
        testPutObject
        testGetObject
        testDeleteObject
        testPutObjectAsync
        testGetObjectAsync
#if defined(ATOMIC_WRITES)
        testDeleteObject
        testPutObjectAtomic
        testGetObject
        testDeleteObject
        testPutObjectAtomicAsync
#endif

    describe "Locking" $ do
        testSharedLock
        testExclusiveLock

runTestPool :: Pool a -> IO a
runTestPool =
    runConnect Nothing (parseConfig "/etc/ceph/ceph.conf") . runPool "test"

testConnectionHost, testPutObject, testGetObject, testDeleteObject :: Spec
testGetObjectAsync, testPutObjectAsync, testSharedLock, testConnectBug :: Spec
testExclusiveLock :: Spec
#if defined(ATOMIC_WRITES)
testPutObjectAtomic, testPutObjectAtomicAsync :: Spec
#endif

testConnectBug =
    it "does not segfault" $ do
        -- Make 8 connections at once
        as <- replicateM 8 $
            Async.async $ runTestPool $ return ()
        mapM Async.wait as
        assertBool "Failed" True

testConnectionHost =
    it "able to establish connetion to local Ceph cluster" $ do
        runTestPool $ return ()
        assertBool "Failed" True


testPutObject =
    it "write object accepted by storage cluster" $ do
        runTestPool . runObject "test/TestSuite.hs" $ do
            writeFull "schrodinger's hai?\n"
            writeChunk 14 "cat"
        assertBool "Failed" True

testGetObject =
    it "read object returns correct data" $ do
        a <- runTestPool $ runObject "test/TestSuite.hs" $ readChunk 32 0
        either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") a
        (Right b) <- runTestPool . runObject "test/TestSuite.hs" $ readFull
        assertEqual "Incorrect content readFull" "schrodinger's cat?\n" b

testDeleteObject =
    it "deletes the object afterward" $ do
        runTestPool $ runObject "test/TestSuite.hs" remove
        assertBool "Failed" True

testPutObjectAsync =
    it "write object accepted by storage cluster" $ do
        runTestPool . runAsync $ runObject "test/TestSuite.hs" $ do
            wr <- writeFull "schrodinger's hai?\n"
            print . isNothing <$> waitSafe wr
            wr' <- writeChunk 14 "cat"
            waitSafe wr'
        assertBool "Failed" True

testGetObjectAsync =
    it "read object returns correct data" $ do
        r <- runTestPool . runAsync . runObject "test/TestSuite.hs" $ readChunk 32 0 >>= look
        either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") r
        r' <- runTestPool . runAsync . runObject "test/TestSuite.hs" $ readFull >>= look
        either throwIO (assertEqual "readChunk" "schrodinger's cat?\n") r'

#if defined(ATOMIC_WRITES)
testPutObjectAtomicAsync =
    it "atomically writes data" $ do
        e <- runTestPool . runAsync . runObject "test/TestSuite.hs" $ do
            write <- runAtomicWrite $ do
                setXAttribute "Pony" "blue"
                compareXAttribute "Pony" eq "pink"
                writeFull "schrodinger's hai?\n"
                writeChunk 14 "cat"
            waitSafe write
        assertBool "Write did not fail" (isJust e)

testPutObjectAtomic =
    it "atomically writes data" $ do
        e <- runTestPool . runObject "test/TestSuite.hs" . runAtomicWrite $ do
            writeFull "schrodinger's hai?\n"
            writeChunk 14 "cat"
        assertBool "Write failed" (isNothing e)
#endif

testSharedLock =
    it "locks and unlocks quickly" $ do
        (_ :: Either SomeException a) <- try $ runTestPool $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 4.3) $
                error "Bang!"
        (_ :: Either SomeException a) <- try $ runTestPool $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 60) $
            withSharedLock "test/TestSuite.hs" "lock" "description" "tag" (Just 60) $
                error "Bang again!"
        assertBool "No deadlock" True

testExclusiveLock =
    it "locks and unlocks quickly" $ do
        (_ :: Either SomeException a) <- try $ runTestPool $
            withExclusiveLock "test/TestSuite.hs" "lock" "description" (Just 4.3) $
                error "Bang!"
        (_ :: Either SomeException a) <- try $ runTestPool $
            withExclusiveLock "test/TestSuite.hs" "lock" "description" (Just 4.3) $
                error "Bang!"
        assertBool "No deadlock" True
