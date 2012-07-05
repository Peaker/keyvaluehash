{-# OPTIONS -Wall #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import Data.ByteString.Char8(pack)
import Data.Monoid
import Database.KeyValueHash
import System.Directory(removeDirectoryRecursive)
import qualified Control.Exception as E

ignoreExceptions :: IO () -> IO ()
ignoreExceptions = (`E.catch` \(E.SomeException _) -> return ())

main :: IO ()
main = do
  ignoreExceptions $ removeDirectoryRecursive "/tmp/tmpdb"
  db <- createDatabase "/tmp/tmpdb" stdHash $ mkSize 128
  replicateM_ 2  . forM_ (replicateM 4 ['a'..'z']) $ \s -> do
    let bs = pack s
    writeKey db bs $ mappend bs "val"
    _ <- readKey db bs
    return ()
