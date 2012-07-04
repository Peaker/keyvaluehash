{-# LANGUAGE DeriveDataTypeable, ScopedTypeVariables #-}
module Database.FileArray
  ( FileArray
  , create, open, close
  , Element(..)
  , unsafeElement -- no index checks
  ) where

import Control.Monad (when)
import Data.Bits (Bits, (.&.), complement)
import Data.Typeable (Typeable)
import Data.Word (Word64)
import Foreign.ForeignPtr (ForeignPtr, finalizeForeignPtr, withForeignPtr)
import Foreign.Ptr (plusPtr)
import Foreign.Storable (Storable)
import qualified Control.Exception as Exc
import qualified Foreign.Storable as Storable
import qualified System.IO as IO
import qualified System.IO.MMap as MMap

data FileArray a = FileArray
  { _faCount :: Word64
  , faPtr :: ForeignPtr a
  }

data Element a = Element
  { read :: IO a
  , write :: a -> IO ()
  }

alignPage :: Bits a => a -> a
alignPage x = (x + 0xFFF) .&. complement 0xFFF

sizeOf :: Storable a => a -> Word64
sizeOf = fromIntegral . Storable.sizeOf

create :: forall a. Storable a => FilePath -> Word64 -> IO (FileArray a)
create filePath count = do
  IO.withBinaryFile filePath IO.ReadWriteMode $ \handle ->
    IO.hSetFileSize handle . alignPage . fromIntegral $
    count * elemSize
  open filePath count
  where
    elemSize = sizeOf (undefined :: a)

data MMapWrongRange = MMapWrongRange deriving (Show, Typeable)
instance Exc.Exception MMapWrongRange

open :: forall a. Storable a => FilePath -> Word64 -> IO (FileArray a)
open filePath count = do
  (ptr, base, mapSize) <-
    MMap.mmapFileForeignPtr filePath MMap.ReadWrite Nothing
  when (base > 0 || fromIntegral mapSize < minFileSize) $
    Exc.throwIO MMapWrongRange
  return $ FileArray count ptr
  where
    minFileSize = count * sizeOf (undefined :: a)

close :: FileArray a -> IO ()
close = finalizeForeignPtr . faPtr

{-# INLINE elementSize #-}
elementSize :: forall a. Storable a => FileArray a -> Word64
elementSize _ = sizeOf (undefined :: a)

{-# INLINE unsafeElement #-}
{-# SPECIALIZE unsafeElement :: FileArray Word64 -> Word64 -> IO (Element Word64) #-}
unsafeElement :: Storable a => FileArray a -> Word64 -> IO (Element a)
unsafeElement fileArray ix =
  withForeignPtr (faPtr fileArray) $ \keysPtr -> do
    let ptr = keysPtr `plusPtr` fromIntegral (ix * elementSize fileArray)
    return $ Element (Storable.peek ptr) (Storable.poke ptr)
