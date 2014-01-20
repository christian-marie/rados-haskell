-- |
-- The underlying Base methods used for the monadic implementation
-- "System.Rados.Monadic".
module System.Rados.Internal
(
-- *Type definitions
    Connection,
    Completion,
    Pool,
    WriteOperation,
    F.TimeVal(..),
    F.LockFlag,
    F.ComparisonFlag,
-- *Connecting
    newConnection,
    cleanupConnection,
    confReadFile,
    confParseArgv,
    confParseEnv,
    connect,
-- *Writing
    newPool,
    cleanupPool,
-- **Synchronous
    syncWrite,
    syncWriteFull,
    syncRead,
    syncAppend,
    syncRemove,
    syncStat,
-- **Asynchronous
    newCompletion,
    waitForComplete,
    waitForSafe,
    asyncWrite,
    asyncWriteFull,
    asyncRead,
    asyncAppend,
    asyncStat,
    asyncRemove,
    getAsyncError,
-- **Locking
    exclusiveLock,
    sharedLock,
    unlock,
    F.idempotent,
-- ** Atomic write operations
    newWriteOperation,
    writeOperationAssertExists,
    writeOperationCompareXAttribute,
    F.nop, F.eq, F.ne, F.gt, F.gte, F.lt, F.lte,
    writeOperationSetXAttribute,
    writeOperationRemoveXAttribute,
    writeOperationCreate,
    writeOperationRemove,
    writeOperationWrite,
    writeOperationWriteFull,
    writeOperationAppend,
    writeOperate,
    asyncWriteOperate,
) where

import System.Rados.Error
import qualified System.Rados.FFI as F

import Control.Applicative
import Control.Monad(void)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import Foreign hiding (Pool, newPool, void)
import Foreign.C.String
import Foreign.C.Types
import System.Posix.Types(EpochTime)

-- | A connection to a rados cluster, required to get a 'Pool'
newtype Connection = Connection (Ptr F.RadosT)

-- | An IO context with a rados pool.
newtype Pool = Pool (Ptr F.RadosIOCtxT)

-- | A handle to query the status of an asynchronous action
newtype Completion = Completion (ForeignPtr F.RadosCompletionT)
    deriving (Ord, Eq)

-- | A write operation groups many individal operations together to be executed
-- atomically
newtype WriteOperation = WriteOperation (ForeignPtr F.RadosWriteOpT)

-- |
-- Attempt to create a new 'Connection, taking an optional id.
-- You must run cleanupConnection on the handle when you are done with it.
--
-- @
-- h  <- newConnection Nothing
-- h' <- newConnection $ Just \"admin\"
--
-- cleanupConnection h
-- cleanupConnection h'
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_create>
newConnection :: Maybe B.ByteString -> IO Connection
newConnection maybe_bs =
    alloca $ \rados_t_p_p -> do
        checkError_ "rados_create" $ case maybe_bs of
            Nothing ->
                F.c_rados_create rados_t_p_p nullPtr
            Just bs -> B.useAsCString bs $ \cstr ->
                F.c_rados_create rados_t_p_p cstr
        Connection <$> peek rados_t_p_p

-- |
-- Clean up a cluster handle.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_shutdown>
cleanupConnection :: Connection -> IO ()
cleanupConnection (Connection rados_t_p) =
    F.c_rados_shutdown rados_t_p

-- |
-- Load a config specified by 'FilePath' into a given 'Connection.
--
-- @
-- h <- newConnection Nothing
-- confReadFile h \"\/etc\/config\"
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_conf_read_file>
confReadFile :: Connection -> FilePath -> IO (Maybe RadosError)
confReadFile (Connection rados_t_p) file_path =
    maybeError "rados_conf_read_file" $ withCString file_path $ \cstr ->
        F.c_rados_conf_read_file rados_t_p cstr

confParseArgv :: Connection -> IO (Maybe RadosError)
confParseArgv (Connection rados_t_p) =
    alloca $ \p_argc ->
    alloca $ \ p_argv -> do
        F.c_getProgArgv p_argc p_argv
        argc <- peek p_argc
        argv <- peek p_argv
        maybeError "rados_conf_parse_argv" $
            F.c_rados_conf_parse_argv rados_t_p argc argv

-- The contents of the environment variable are parsed as if they were Ceph
-- command line options. If the CEPH_ARGS environment variable is
-- used.
confParseEnv :: Connection -> IO (Maybe RadosError)
confParseEnv (Connection rados_t_p) =
        maybeError "rados_conf_parse_env" $
            F.c_rados_conf_parse_env rados_t_p nullPtr


-- |
-- Attempt to connect a configured 'Connection.
--
-- @
-- h <- newConnection Nothing
-- confReadFile h \"/etc/config\"
-- connect h
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_connect>
connect :: Connection -> IO ()
connect (Connection rados_t_p) =
    checkError_ "rados_connect" $ F.c_rados_connect rados_t_p

-- |
-- Attempt to create a new 'Pool, requires a valid 'Connection and
-- pool name.
--
-- @
-- h <- newConnection Nothing
-- h ctx <- newPool h "thing"
-- cleanupPool ctx
-- @
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_create>
newPool :: Connection -> B.ByteString -> IO Pool
newPool (Connection rados_t_p) bs =
    B.useAsCString bs $ \cstr ->
    alloca $ \ioctx_p_p -> do
        checkError_ "rados_ioctx_create" $
            F.c_rados_ioctx_create rados_t_p cstr ioctx_p_p
        Pool <$> peek ioctx_p_p

-- |
-- Clean up an Pool
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_ioctx_destroy>
cleanupPool :: Pool -> IO ()
cleanupPool (Pool p) = F.c_rados_ioctx_destroy p

-- |
-- Attempt to create a new 'Completion' that can be used with async IO actions.
--
-- Completion will automatically be cleaned up when it goes out of scope
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_create_completion>
newCompletion :: IO Completion
newCompletion =
    alloca $ \completion_p_p -> do
        checkError_ "rados_aio_create_completion" $
            F.c_rados_aio_create_completion nullPtr nullFunPtr nullFunPtr completion_p_p
        Completion <$> (peek completion_p_p >>= newForeignPtr F.c_rados_aio_release)

-- |
-- Block until a completion is complete. I.e. the operation associated with
-- the completion is at least in memory on all replicas.
--
-- @
-- ...
-- asyncWrite ctx c "oid" 42 \"written at offset fourty-two\"
-- putStrLn \"Waiting for your bytes to get there...\"
-- waitForComplete c
-- putStrLn \"I totally wrote it! Maybe.\"
-- @
--
-- Calls rados_aio_wait_for_complete:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_complete>
waitForComplete :: Completion -> IO ()
waitForComplete (Completion rados_completion_t_fp) = void $
    withForeignPtr rados_completion_t_fp F.c_rados_aio_wait_for_complete

-- |
-- Block until a completion is safe. I.e. the operation associated with
-- the completion is on stable storage on all replicas.
--
-- @
-- ...
-- waitForSafe c
-- putStrLn \"Yeah. Totally did write it. I hope.\"
-- @
--
-- Calls rados_aio_wait_for_safe:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_wait_for_safe>
waitForSafe :: Completion -> IO ()
waitForSafe (Completion rados_completion_t_fp) = void $
    withForeignPtr rados_completion_t_fp F.c_rados_aio_wait_for_complete

getAsyncError :: Completion -> IO (Either RadosError Int)
getAsyncError (Completion rados_completion_t_fp) = do
    checkError' "rados_aio_get_return_value" $
        withForeignPtr rados_completion_t_fp $
            F.c_rados_aio_get_return_value

asyncRead :: Pool
          -> Completion
          -> B.ByteString
          -> Word64
          -> Word64
          -> IO (Either RadosError B.ByteString)
asyncRead (Pool ioctx_p) (Completion rados_completion_t_fp) oid len offset = do
    c_buf <- mallocBytes (fromIntegral len)
    B.useAsCString oid $ \c_oid -> do
        let c_len    = fromIntegral len
        let c_offset = fromIntegral offset
        result <- checkError' "rados_aio_read" $
            withForeignPtr rados_completion_t_fp $ \cmp_p ->
                F.c_rados_aio_read ioctx_p c_oid cmp_p c_buf c_len c_offset
        case result of
            Right _ ->
                Right <$> B.unsafePackCStringLen (c_buf, fromIntegral len)
            Left e ->
                return . Left $ e

-- |
-- From right to left, this function reads as:
--
-- Write ByteString bytes to Word64 offset at oid ByteString, notifying this
-- Completion, all within this Pool
--
-- @
-- ...
-- ctx <- newPool h \"thing\"
-- asyncWrite ctx c "oid" 42 \"written at offset fourty-two\"
-- putStrLn $ "I wrote " ++ show n ++ " bytes"
-- @
--
-- Calls rados_aio_write:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write>
asyncWrite
    :: Pool
    -> Completion
    -> B.ByteString
    -> Word64
    -> B.ByteString
    -> IO (Either RadosError Int)
asyncWrite (Pool ioctxt_p) (Completion rados_completion_t_fp)
           oid offset bs =
    B.useAsCString oid   $ \c_oid ->
    withForeignPtr rados_completion_t_fp $ \cmp_p ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = fromIntegral offset
        checkError' "rados_aio_write" $ F.c_rados_aio_write
            ioctxt_p c_oid cmp_p c_buf c_size c_offset
-- |
-- Same calling convention as asyncWrite, simply omitting an offset.
-- This will truncate any existing object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_write_full>
asyncWriteFull
    :: Pool
    -> Completion
    -> B.ByteString
    -> B.ByteString
    -> IO (Either RadosError Int)
asyncWriteFull (Pool ioctxt_p) (Completion rados_completion_t_fp) oid bs =
    B.useAsCString oid   $ \c_oid ->
    withForeignPtr rados_completion_t_fp $ \cmp_p ->
    useAsCStringCSize bs $ \(c_buf, c_size) ->
        checkError' "rados_aio_write_full" $
            F.c_rados_aio_write_full
                ioctxt_p c_oid cmp_p c_buf c_size

-- |
-- Same calling convention as asyncWriteFull, simply appends to an object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_append>
asyncAppend
    :: Pool
    -> Completion
    -> B.ByteString
    -> B.ByteString
    -> IO (Either RadosError Int)
asyncAppend (Pool ioctxt_p) (Completion rados_completion_t_fp) oid bs =
    B.useAsCString oid        $ \c_oid ->
    withForeignPtr rados_completion_t_fp $ \cmp_p ->
    useAsCStringCSize bs $ \(c_buf, c_size) ->
        checkError' "rados_aio_append" $ F.c_rados_aio_append
            ioctxt_p c_oid cmp_p c_buf c_size

-- |
-- Same calling convention as asyncWriteFull, simply appends to an object.
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_append>
asyncRemove
    :: Pool
    -> Completion
    -> B.ByteString
    -> IO (Either RadosError Int)
asyncRemove (Pool ioctxt_p) (Completion rados_completion_t_fp) oid =
    B.useAsCString oid $ \c_oid ->
    withForeignPtr rados_completion_t_fp $ \cmp_p ->
        checkError' "rados_aio_append" $ F.c_rados_aio_remove
            ioctxt_p c_oid cmp_p

-- |
-- Request the file size and mtime, returns two pointers to the data that will
-- be able to be peeked at when the request is complete.
--
-- These pointers will free themselves
--
-- Calls:
-- <http://ceph.com/docs/master/rados/api/librados/#rados_aio_stat>
asyncStat
    :: Pool
    -> Completion
    -> B.ByteString
    -> IO (Either RadosError (ForeignPtr Word64, ForeignPtr CTime))
asyncStat (Pool ioctxt_p) (Completion rados_completion_t_fp) oid =
    B.useAsCString oid $ \c_oid -> do
        size_fp <- mallocForeignPtr
        time_fp <- mallocForeignPtr
        withForeignPtr rados_completion_t_fp $ \cmp_p -> do
            result <- withForeignPtr size_fp $ \p_size ->
                withForeignPtr time_fp $ \p_time ->
                    checkError' "rados_aio_stat" $
                        F.c_rados_aio_stat ioctxt_p c_oid cmp_p p_size p_time
            return $ either Left (const $ Right (size_fp, time_fp)) result


-- |
-- Write a 'ByteString' to 'Pool', object id and offset.
--
-- @
-- ...
-- syncWrite Pool \"object_id\" 42 \"written at offset fourty-two\"
-- @
syncWrite
    :: Pool
    -> B.ByteString
    -> Word64
    -> B.ByteString
    -> IO (Maybe RadosError)
syncWrite (Pool ioctxt_p) oid offset bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) -> do
        let c_offset = fromIntegral offset
        maybeError "rados_write" $ F.c_rados_write
            ioctxt_p c_oid c_buf c_size c_offset

-- |
-- Write a 'ByteString' to 'Pool' and object id.
--
-- This will replace any existing object at the same 'Pool' and object id.
syncWriteFull
    :: Pool
    -> B.ByteString
    -> B.ByteString
    -> IO (Maybe RadosError)
syncWriteFull (Pool ioctxt_p) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, len) ->
        maybeError "rados_write_full" $ F.c_rados_write_full
            ioctxt_p c_oid c_buf len

-- |
-- Append a 'ByteString' to 'Pool' and object id.
--
-- Returns the number of bytes written.
syncAppend
    :: Pool
    -> B.ByteString
    -> B.ByteString
    -> IO (Maybe RadosError)
syncAppend (Pool ioctxt_p) oid bs =
    B.useAsCString oid   $ \c_oid ->
    useAsCStringCSize bs $ \(c_buf, c_size) ->
        maybeError "rados_append" $ F.c_rados_append
            ioctxt_p c_oid c_buf c_size

-- |
-- Read from 'Pool', object ID and offset n bytes.
--
-- There is no async read provided by this binding.
--
-- TODO: Document a multithreaded example to compensate for no async. This may
-- or may not require to be called from the same OS thread. Test and document
-- that.
--
-- Or, even, provide an abstract Future type that can simply wrap an mvar, then
-- do the multithreading for the user.
--
-- @
--         ...
--         -- Read 100 bytes into bs from offset 42
--         bs <- syncRead pool \"object_id\" 42 100
--         ...
-- @
syncRead
    :: Pool
    -> B.ByteString
    -> Word64
    -> Word64
    -> IO (Either RadosError B.ByteString)
syncRead (Pool ioctxt_p) oid len offset = do
    c_buf <- mallocBytes (fromIntegral len)
    B.useAsCString oid             $ \c_oid -> do
        let c_offset = fromIntegral offset
        let c_len    = fromIntegral len
        result <- checkError' "rados_read" $
            F.c_rados_read ioctxt_p c_oid c_buf c_len c_offset
        case result of
            Right read_bytes ->
                Right <$> B.unsafePackCStringLen (c_buf, read_bytes)
            Left e ->
                return . Left $ e

useAsCStringCSize :: B.ByteString -> ((CString, CSize) -> IO a) -> IO a
useAsCStringCSize bs f =
    B.useAsCStringLen bs $ \(cstr, len) -> f (cstr, (CSize . fromIntegral) len)

-- |
-- Delete an object from 'Pool' by ID.
--
syncRemove :: Pool -> B.ByteString -> IO (Maybe RadosError)
syncRemove (Pool ioctxt_p) oid =
    B.useAsCString oid $ \c_oid ->
        maybeError "rados_remove" $ F.c_rados_remove
            ioctxt_p c_oid

syncStat :: Pool -> B.ByteString -> IO (Either RadosError (Word64, EpochTime))
syncStat (Pool ioctxt_p) oid =
    B.useAsCString oid $ \c_oid ->
    alloca $ \size_p ->
    alloca $ \time_p -> do
        result <- checkError' "rados_stat" $ F.c_rados_stat
            ioctxt_p c_oid size_p time_p
        case result of
            Left e -> return $ Left e
            Right _ -> do
                Right <$> liftA2 (,) (peek size_p) (peek time_p)

    
combineLockFlags :: [F.LockFlag] -> F.LockFlag
combineLockFlags = F.LockFlag . foldr ((.|.) . F.unLockFlag) 0

timeValFromRealFrac :: RealFrac n => n -> F.TimeVal
timeValFromRealFrac n = do
    let (seconds, fractional) = properFraction n
    F.TimeVal seconds (floor $ 1000000 / fractional)
-- |
-- Make an exclusive lock
exclusiveLock
    :: RealFrac duration
    => Pool
    -> B.ByteString -- ^ oid
    -> B.ByteString -- ^ name
    -> B.ByteString -- ^ cookie
    -> B.ByteString -- ^ desc
    -> Maybe duration
    -> [F.LockFlag]
    -> IO ()
exclusiveLock (Pool ioctx_p) oid name cookie desc maybe_duration flags =
    let flag = combineLockFlags flags in
        B.useAsCString oid    $ \c_oid ->
        B.useAsCString name   $ \c_name ->
        B.useAsCString cookie $ \c_cookie ->
        B.useAsCString desc   $ \c_desc ->
        case maybe_duration of
            Nothing -> 
                checkErrorRetryBusy_ "c_rados_lock_exclusive" $
                    F.c_rados_lock_exclusive ioctx_p
                                            c_oid
                                            c_name
                                            c_cookie
                                            c_desc
                                            nullPtr
                                            flag
            Just duration ->
                alloca $ \timeval_p -> do
                    let timeval = timeValFromRealFrac duration
                    poke timeval_p timeval
                    checkErrorRetryBusy_ "c_rados_lock_exclusive" $
                        F.c_rados_lock_exclusive ioctx_p
                                                 c_oid
                                                 c_name
                                                 c_cookie
                                                 c_desc
                                                 timeval_p
                                                 flag

-- |
-- Make a shared lock
sharedLock
    :: RealFrac duration
    => Pool
    -> B.ByteString -- ^ oid
    -> B.ByteString -- ^ name
    -> B.ByteString -- ^ cookie
    -> B.ByteString -- ^ tag
    -> B.ByteString -- ^ desc
    -> Maybe duration
    -> [F.LockFlag]
    -> IO ()
sharedLock (Pool ioctx_p) oid name cookie tag desc maybe_duration flags =
    let flag = combineLockFlags flags in
        B.useAsCString oid    $ \c_oid ->
        B.useAsCString name   $ \c_name ->
        B.useAsCString cookie $ \c_cookie ->
        B.useAsCString tag    $ \c_tag ->
        B.useAsCString desc   $ \c_desc ->
        case maybe_duration of
            Nothing -> 
                checkErrorRetryBusy_ "c_rados_lock_shared" $
                    F.c_rados_lock_shared ioctx_p
                                            c_oid
                                            c_name
                                            c_cookie
                                            c_tag
                                            c_desc
                                            nullPtr
                                            flag
            Just duration ->
                alloca $ \timeval_p -> do
                    let timeval = timeValFromRealFrac duration
                    poke timeval_p timeval
                    checkErrorRetryBusy_ "c_rados_lock_shared" $
                        F.c_rados_lock_shared ioctx_p
                                                 c_oid
                                                 c_name
                                                 c_cookie
                                                 c_tag
                                                 c_desc
                                                 timeval_p
                                                 flag
--
-- |
-- Release a lock of any sort
unlock
    :: Pool
    -> B.ByteString -- ^ oid
    -> B.ByteString -- ^ name
    -> B.ByteString -- ^ cookie
    -> IO ()
unlock (Pool ioctx_p) oid name cookie =
    B.useAsCString oid    $ \c_oid ->
    B.useAsCString name   $ \c_name ->
    B.useAsCString cookie $ \c_cookie ->
        checkError_ "c_rados_unlock" $
            F.c_rados_unlock ioctx_p
                             c_oid
                             c_name
                             c_cookie

newWriteOperation
    :: IO WriteOperation
newWriteOperation = 
    WriteOperation <$> (F.c_rados_create_write_op >>=
                        newForeignPtr F.c_rados_release_write_op)

writeOperationAssertExists
    :: WriteOperation
    -> IO ()
writeOperationAssertExists (WriteOperation o) =
    withForeignPtr o F.c_rados_write_op_assert_exists 

writeOperationCompareXAttribute
    :: WriteOperation
    -> B.ByteString
    -> F.ComparisonFlag
    -> B.ByteString
    -> IO () 
writeOperationCompareXAttribute (WriteOperation o) key comparison_flag value =
    withForeignPtr o $ \ofp ->
    B.useAsCString key $ \c_key -> 
    B.useAsCStringLen value $ \(c_val, c_val_len) -> do
        F.c_rados_write_op_cmpxattr
            ofp c_key comparison_flag c_val (fromIntegral c_val_len)

writeOperationSetXAttribute
    :: WriteOperation
    -> B.ByteString
    -> B.ByteString
    -> IO () 
writeOperationSetXAttribute (WriteOperation o) key value =
    withForeignPtr o $ \ofp ->
    B.useAsCString key $ \c_key -> 
    B.useAsCStringLen value $ \(c_val, c_val_len) -> do
        F.c_rados_write_op_setxattr ofp c_key c_val (fromIntegral c_val_len)


writeOperationRemoveXAttribute
    :: WriteOperation
    -> B.ByteString
    -> IO () 
writeOperationRemoveXAttribute (WriteOperation o) key =
    withForeignPtr o $ \ofp ->
    B.useAsCString key $ \c_key -> 
        F.c_rados_write_op_rmxattr ofp c_key

writeOperationCreate
    :: WriteOperation
    -> Bool
    -> IO () 
writeOperationCreate (WriteOperation o) exclusive =
    withForeignPtr o $ \ofp ->
        let int_exclusive = if exclusive then 1 else 0 in
            F.c_rados_write_op_create ofp int_exclusive nullPtr

writeOperationRemove
    :: WriteOperation
    -> IO () 
writeOperationRemove (WriteOperation o) =
    withForeignPtr o $ \ofp ->
        F.c_rados_write_op_remove ofp 

writeOperationWrite
    :: WriteOperation
    -> B.ByteString
    -> Word64
    -> IO ()
writeOperationWrite (WriteOperation o) buffer offset = 
    withForeignPtr o $ \ofp ->
    B.useAsCStringLen buffer $ \(c_buf, c_len) ->
        F.c_rados_write_op_write ofp c_buf (fromIntegral c_len) offset

writeOperationWriteFull
    :: WriteOperation
    -> B.ByteString
    -> IO ()
writeOperationWriteFull (WriteOperation o) buffer = 
    withForeignPtr o $ \ofp ->
    B.useAsCStringLen buffer $ \(c_buf, c_len) ->
        F.c_rados_write_op_write_full ofp c_buf (fromIntegral c_len)


writeOperationAppend
    :: WriteOperation
    -> B.ByteString
    -> IO ()
writeOperationAppend (WriteOperation o) buffer = 
    withForeignPtr o $ \ofp ->
    B.useAsCStringLen buffer $ \(c_buf, c_len) ->
        F.c_rados_write_op_append ofp c_buf (fromIntegral c_len)

writeOperate
    :: WriteOperation
    -> Pool
    -> B.ByteString
    -> IO (Maybe RadosError)
writeOperate (WriteOperation o) (Pool ioctx_p) oid = 
    withForeignPtr o $ \ofp ->
    B.useAsCString oid $ \c_oid->
        maybeError "rados_write_op_operate" $
            F.c_rados_write_op_operate ofp ioctx_p c_oid nullPtr

asyncWriteOperate
    :: WriteOperation
    -> Pool
    -> Completion
    -> B.ByteString
    -> IO (Either RadosError Int) 
asyncWriteOperate (WriteOperation o) (Pool ioctx_p)
                  (Completion rados_completion_t_fp) oid =
    withForeignPtr o $ \ofp ->
    withForeignPtr rados_completion_t_fp $ \cmp_p ->
    B.useAsCString oid $ \c_oid->
        checkError' "rados_aio_write_op_operate" $
            F.c_rados_aio_write_op_operate ofp ioctx_p cmp_p c_oid nullPtr
