{-# LANGUAGE ForeignFunctionInterface #-}
module System.IO.MMap.Sync (msync, SyncFlag(..), InvalidateFlag(..)) where

import Control.Monad (when)
import Data.Int (Int64)
import Foreign.C.Error (throwErrno)
import Foreign.C.Types (CInt(..), CSize(..))
import Foreign.Ptr (Ptr)

-- TODO: This should be in the mmap package

data SyncFlag = Async | Sync
  deriving (Eq, Ord, Read, Show)
data InvalidateFlag = Invalidate | NoInvalidate
  deriving (Eq, Ord, Read, Show)

foreign import ccall unsafe "HsMsync.h system_io_msync"
    c_system_io_msync :: Ptr a -> CSize -> CInt -> IO CInt

mkMSyncFlag :: Maybe SyncFlag -> CInt
mkMSyncFlag Nothing = 0
mkMSyncFlag (Just Sync) = 1
mkMSyncFlag (Just Async) = 2

mkInvalidateFlag :: InvalidateFlag -> CInt
mkInvalidateFlag NoInvalidate = 0
mkInvalidateFlag Invalidate = 4

mkFlags :: Maybe SyncFlag -> InvalidateFlag -> CInt
mkFlags mSyncFlag invalidateFlag =
  mkMSyncFlag mSyncFlag +
  mkInvalidateFlag invalidateFlag

msync :: Ptr a -> Int64 -> Maybe SyncFlag -> InvalidateFlag -> IO ()
msync ptr size mSyncFlag invalidateFlag = do
  res <-
    c_system_io_msync ptr (fromIntegral size) $
    mkFlags mSyncFlag invalidateFlag
  when (res == -1) $ throwErrno "msync failed"
