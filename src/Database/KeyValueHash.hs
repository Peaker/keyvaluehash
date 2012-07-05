{-# LANGUAGE TemplateHaskell, DeriveDataTypeable #-}
module Database.KeyValueHash
  ( Key, Value
  , Size, mkSize, sizeLinear
  , HashFunction, stdHash, mkHashFunc
  , Database, createDatabase, openDatabase, closeDatabase
  , withCreateDatabase, withOpenDatabase
  , readKey, writeKey, deleteKey
  ) where

import Control.Applicative ((<$>), (<*>))
import Control.Monad (when)
import Data.Binary (Binary(..))
import Data.Binary.Get (runGet)
import Data.Binary.Put (runPut)
import Data.Derive.Binary(makeBinary)
import Data.DeriveTH(derive)
import Data.Hashable (Hashable, hash)
import Data.List (intercalate)
import Data.Monoid (mconcat)
import Data.Typeable (Typeable)
import Data.Word (Word8, Word32, Word64)
import Database.FileArray (FileArray)
import Database.GrowingFile (GrowingFile)
import System.FilePath ((</>))
import qualified Control.Exception as Exc
import qualified Data.ByteString as SBS
import qualified Data.ByteString.Lazy as LBS
import qualified Database.FileArray as FileArray
import qualified Database.GrowingFile as GrowingFile
import qualified System.Directory as Directory

valuesGrowthSize :: Word64
valuesGrowthSize = 128 * 1024

-- of any length, but long values may be less efficiently handled
type Key = SBS.ByteString
type Value = SBS.ByteString

newtype Size = Size {
  sizeLog :: Word8
  }

sizeLinear :: Size -> Word64
sizeLinear = (2^) . sizeLog

instance Show Size where
  show (Size x) = "sz" ++ show x

mkSize :: Word64 -> Size
mkSize = Size . (ceiling :: Double -> Word8) . logBase 2 . fromIntegral

data HashFunction = HashFunction
  { hfHash :: Key -> Size -> Word64
  , hfName :: String
  }

-- Must not use 0-value because it is used as an invalid value ptr
cap :: Word64 -> Size -> Word64
cap val size = val `mod` (sizeLinear size)

mkHashFunc :: String -> (Key -> Word64) -> HashFunction
mkHashFunc name f = HashFunction
  { hfName = name
  , hfHash = cap . f
  }

stdHash :: HashFunction
stdHash = mkHashFunc "Hashable" (fromIntegral . hash)

type ValuePtr = Word64 -- offset in values file
type KeyPtr = Word64 -- index in key file
type KeyRecord = ValuePtr

data FileRange = FileRange
  { frOffset :: Word64
  , frSize :: Word64
  } deriving (Show)
derive makeBinary ''FileRange

data ValueHeader = ValueHeader
  { vhNextCollision :: ValuePtr
    -- if value is re-written multiple times, we can do it in-place as long as it fits
  , vhAllocSize :: Word32
  , vhKeySize :: Word32
  , vhValueSize :: Word32
  }
derive makeBinary ''ValueHeader

data Database = Database
  { _dbPath :: FilePath -- directory container
  , dbSize :: Size
  , dbHashFunc :: HashFunction
  , dbKeysArray :: FileArray KeyRecord
  , dbValues :: GrowingFile
  }

atVhNextCollision :: (ValuePtr -> ValuePtr) -> ValueHeader -> ValueHeader
atVhNextCollision f v = v { vhNextCollision = f (vhNextCollision v) }

encode :: Binary a => a -> SBS.ByteString
encode = strictify . runPut . put

decode :: Binary a => SBS.ByteString -> a
decode = runGet get . lazify

binaryPutSize :: Binary a => a -> Word64
binaryPutSize = fromIntegral . SBS.length . encode

valueHeaderSize :: Word64
valueHeaderSize = binaryPutSize $ ValueHeader 0 0 0 0

makeFileName :: String -> FilePath -> HashFunction -> Size -> FilePath
makeFileName prefix path func size =
  path </> intercalate "_" [prefix, hfName func, show size]

keysFileName :: FilePath -> HashFunction -> Size -> FilePath
keysFileName = makeFileName "keys"

valuesFileName :: FilePath -> HashFunction -> Size -> FilePath
valuesFileName = makeFileName "values"

data FileAlreadyExists = FileAlreadyExists String deriving (Show, Typeable)
instance Exc.Exception FileAlreadyExists
assertNotExists :: FilePath -> IO ()
assertNotExists fileName = do
  de <- Directory.doesDirectoryExist fileName
  fe <- Directory.doesFileExist fileName
  when (de || fe) $ Exc.throwIO (FileAlreadyExists fileName)

createDatabase :: FilePath -> HashFunction -> Size -> IO Database
createDatabase path func size = do
  Directory.createDirectory path
  mapM_ assertNotExists [keysFN, valuesFN]
  Database path size func
    <$> FileArray.create keysFN (sizeLinear size)
    <*> GrowingFile.create valuesFN valuesGrowthSize
  where
    keysFN = keysFileName path func size
    valuesFN = valuesFileName path func size

openDatabase :: FilePath -> HashFunction -> Size -> IO Database
openDatabase path func size =
  Database path size func
    <$> FileArray.open (keysFileName path func size) (sizeLinear size)
    <*> GrowingFile.open (valuesFileName path func size) valuesGrowthSize

closeDatabase :: Database -> IO ()
closeDatabase db = do
  FileArray.close $ dbKeysArray db
  GrowingFile.close $ dbValues db

withCreateDatabase :: FilePath -> HashFunction -> Size -> (Database -> IO a) -> IO a
withCreateDatabase path func size =
  Exc.bracket (createDatabase path func size) closeDatabase

withOpenDatabase :: FilePath -> HashFunction -> Size -> (Database -> IO a) -> IO a
withOpenDatabase path func size =
  Exc.bracket (openDatabase path func size) closeDatabase

hashKey :: Database -> Key -> KeyPtr
hashKey db key = hfHash (dbHashFunc db) key (dbSize db)

invalidValuePtr :: ValuePtr
invalidValuePtr = 0

readRange :: GrowingFile -> FileRange -> IO SBS.ByteString
readRange gfile rng = GrowingFile.readRange gfile (frOffset rng) (frSize rng)

writeRange :: GrowingFile -> Word64 -> SBS.ByteString -> IO ()
writeRange gfile offset bs = GrowingFile.writeRange gfile offset bs

decodeFileRange :: Binary a => GrowingFile -> FileRange -> IO a
decodeFileRange gfile rng = decode <$> readRange gfile rng

strictify :: LBS.ByteString -> SBS.ByteString
strictify = SBS.concat . LBS.toChunks

lazify :: SBS.ByteString -> LBS.ByteString
lazify = LBS.fromChunks . (: [])

data ValuePtrRef = ValuePtrRef
  { vprVal :: ValuePtr
  , vprSet :: ValuePtr -> IO ()
  }

hashValuePtrRef :: Database -> Key -> IO ValuePtrRef
hashValuePtrRef db key = do
  element <- FileArray.unsafeElement (dbKeysArray db) $ hashKey db key
  valuePtr <- FileArray.read element
  return ValuePtrRef
    { vprVal = valuePtr
    , vprSet = FileArray.write element
    }

valueKeyRange :: ValuePtr -> ValueHeader -> FileRange
valueKeyRange valuePtr header =
  FileRange (valuePtr + valueHeaderSize) . fromIntegral $ vhKeySize header

valueDataRange :: ValuePtr -> ValueHeader -> FileRange
valueDataRange valuePtr header =
  FileRange (valuePtr + valueHeaderSize + fromIntegral (vhKeySize header)) . fromIntegral $
  vhValueSize header

findKey :: Database -> Key -> IO (Maybe (ValuePtrRef, ValueHeader))
findKey db key =
  find =<< hashValuePtrRef db key
  where
    find valuePtrRef
      | vprVal valuePtrRef == invalidValuePtr = return Nothing
      | otherwise = do
        valueHeader <-
          decodeFileRange (dbValues db)
          (FileRange (vprVal valuePtrRef) valueHeaderSize)
        vKey <- readRange (dbValues db) $ valueKeyRange (vprVal valuePtrRef) valueHeader
        if key == vKey
          then return $ Just (valuePtrRef, valueHeader)
          else find $ nextCollisionRef (vprVal valuePtrRef) valueHeader
    nextCollisionRef valuePtr valueHeader = ValuePtrRef
        { vprVal = vhNextCollision valueHeader
        , vprSet =
          writeRange (dbValues db) valuePtr .
          encode . flip (atVhNextCollision . const) valueHeader
        }

readKey :: Database -> Key -> IO (Maybe Value)
readKey db key = do
  mValueRange <- findKey db key
  case mValueRange of
    Nothing -> return Nothing
    Just (valuePtrRef, valueHeader) ->
      Just <$> readRange (dbValues db) (valueDataRange (vprVal valuePtrRef) valueHeader)

pairLengths :: Key -> Value -> (Word32, Word32)
pairLengths key value = (keyLen, valueLen)
  where
    keyLen = fromIntegral $ SBS.length key
    valueLen = fromIntegral $ SBS.length value

appendNewValue :: Database -> ValuePtr -> Key -> Value -> IO ValuePtr
appendNewValue db nextCollision key value =
  GrowingFile.append (dbValues db) $ mconcat [headerStr, key, value]
  where
    headerStr = encode ValueHeader
      { vhNextCollision = nextCollision
      , vhAllocSize = keyLen + valueLen
      , vhKeySize = keyLen
      , vhValueSize = valueLen
      }
    (keyLen, valueLen) = pairLengths key value

writeKey :: Database -> Key -> Value -> IO ()
writeKey db key value = do
  mResult <- findKey db key
  case mResult of
    Just (valuePtrRef, valueHeader) ->
      if vhAllocSize valueHeader >= keyLen + valueLen then
        -- re-use existing storage:
        let headerStr = encode valueHeader { vhKeySize = keyLen, vhValueSize = valueLen }
        in writeRange (dbValues db) (vprVal valuePtrRef) $
           mconcat [headerStr, key, value]
      else
        -- The old value now becomes unreachable
        setValue valuePtrRef $ vhNextCollision valueHeader
    Nothing -> do
      hashAnchor <- hashValuePtrRef db key
      setValue hashAnchor $ vprVal hashAnchor
  where
    (keyLen, valueLen) = pairLengths key value
    setValue ref nextCollision =
      vprSet ref =<< appendNewValue db nextCollision key value

deleteKey :: Database -> Key -> IO ()
deleteKey db key = do
  mResult <- findKey db key
  case mResult of
    Just (valuePtrRef, valueHeader) ->
      -- The old value now becomes unreachable
      vprSet valuePtrRef $ vhNextCollision valueHeader
    Nothing ->
      -- TODO: Throw exception?
      return ()
