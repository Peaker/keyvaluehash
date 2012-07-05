{-# LANGUAGE DeriveDataTypeable #-}
module Database.GrowingFile
  ( GrowingFile
  , create, open, close
  , readRange, writeRange
  , append
  ) where

import Control.Applicative (liftA2)
import Control.Monad (when, (<=<))
import Data.IORef
import Data.Typeable (Typeable)
import Data.Word(Word64)
import Foreign (copyBytes)
import Foreign.C.Types (CChar)
import Foreign.ForeignPtr (ForeignPtr, finalizeForeignPtr, withForeignPtr)
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Foreign.Storable (Storable)
import qualified Foreign.Storable.Record as Store
import qualified Control.Exception as Exc
import qualified Data.ByteString as SBS
import qualified Foreign.Storable as Storable
import qualified System.IO as IO
import qualified System.IO.MMap as MMap

data Header = Header
  { hAllocated :: Word64
  , hUsed :: Word64
  } deriving (Show)

store :: Store.Dictionary Header
store =
  Store.run $
  liftA2 Header
  (Store.element hAllocated)
  (Store.element hUsed)

instance Storable Header where
  sizeOf = Store.sizeOf store
  alignment = Store.alignment store
  peek = Store.peek store
  poke = Store.poke store

data GrowingFile = GrowingFile
  { gfFilePath :: FilePath
  , gfPtr :: IORef (ForeignPtr CChar)
  , gfGrowthSize :: Word64
  }

sizeOf :: (Storable a, Integral b) => a -> b
sizeOf = fromIntegral . Storable.sizeOf

resizeFile :: FilePath -> Word64 -> IO ()
resizeFile filePath size =
  IO.withBinaryFile filePath IO.ReadWriteMode $ \handle ->
    IO.hSetFileSize handle (fromIntegral size)

data MMapWrongRange = MMapWrongRange deriving (Show, Typeable)
instance Exc.Exception MMapWrongRange

peekForeignPtr :: Storable b => ForeignPtr a -> IO b
peekForeignPtr fptr =
  withForeignPtr fptr $ \ptr -> Storable.peek (castPtr ptr)

pokeForeignPtr :: Storable b => ForeignPtr a -> b -> IO ()
pokeForeignPtr fptr val =
  withForeignPtr fptr $ \ptr -> Storable.poke (castPtr ptr) val

mmap :: FilePath -> IO (ForeignPtr a)
mmap filePath = do
  (fptr, base, mapSize) <-
    MMap.mmapFileForeignPtr filePath MMap.ReadWrite Nothing

  when (base > 0) $ Exc.throwIO MMapWrongRange
  header <- peekForeignPtr fptr
  when (fromIntegral mapSize < hAllocated header) $ Exc.throwIO MMapWrongRange

  return fptr

open :: FilePath -> Word64 -> IO GrowingFile
open filePath growthSize = do
  fptr <- newIORef =<< mmap filePath
  return $ GrowingFile filePath fptr growthSize

create :: FilePath -> Word64 -> IO GrowingFile
create filePath growthSize = do
  resizeFile filePath firstAllocatedSize
  fptr <- mmap filePath
  pokeForeignPtr fptr $ Header firstAllocatedSize firstUsedSize
  fptrVar <- newIORef fptr
  return $ GrowingFile filePath fptrVar growthSize
  where
    firstAllocatedSize = max growthSize firstUsedSize
    firstUsedSize = sizeOf (undefined :: Header)

readHeader :: GrowingFile -> IO Header
readHeader = peekForeignPtr <=< readIORef . gfPtr

writeHeader :: GrowingFile -> Header -> IO ()
writeHeader gfile header = do
  ptr <- readIORef (gfPtr gfile)
  pokeForeignPtr ptr header

unmap :: GrowingFile -> IO ()
unmap gfile = do
  ptr <- readIORef (gfPtr gfile)
  finalizeForeignPtr ptr

close :: GrowingFile -> IO ()
close = unmap

withPtr :: GrowingFile -> (Ptr CChar -> IO a) -> IO a
withPtr gfile f = do
  fptr <- readIORef (gfPtr gfile)
  withForeignPtr fptr f

readRange :: GrowingFile -> Word64 -> Word64 -> IO SBS.ByteString
readRange gfile start count = withPtr gfile $ \ptr ->
  SBS.packCStringLen (ptr `plusPtr` fromIntegral start, fromIntegral count)

writeRange :: GrowingFile -> Word64 -> SBS.ByteString -> IO ()
writeRange gfile start bs =
  withPtr gfile $ \ptr ->
  SBS.useAsCStringLen bs $
  \(ccharptr, len) -> copyBytes (ptr `plusPtr` fromIntegral start) ccharptr len

align :: Word64 -> Word64 -> Word64
align x boundary = x + (-x) `mod` boundary

computeSize :: GrowingFile -> Word64 -> Word64
computeSize gfile newUsed =
  align newUsed (gfGrowthSize gfile)

resize :: GrowingFile -> Header -> Word64 -> IO ()
resize gfile header newUsed =
  if newUsed > hAllocated header
  then do
    unmap gfile
    let newSize = computeSize gfile newUsed
    resizeFile (gfFilePath gfile) newSize
    writeIORef (gfPtr gfile) =<< mmap (gfFilePath gfile)
    writeHeader gfile Header { hAllocated = newSize, hUsed = newUsed }
  else
    writeHeader gfile $ header { hUsed = newUsed }

append :: GrowingFile -> SBS.ByteString -> IO Word64
append gfile bs = do
  header <- readHeader gfile
  let
    curUsed = hUsed header
    newUsed = curUsed + fromIntegral (SBS.length bs)
  resize gfile header newUsed
  writeRange gfile curUsed bs
  return curUsed
