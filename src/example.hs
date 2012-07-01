{-# OPTIONS -Wall #-}
{-# LANGUAGE OverloadedStrings #-}
import Data.ByteString.Char8 ()
import qualified Control.Exception as Exc
import qualified Data.ByteString as SBS
import qualified Database.Hash as HashDB

dbPath :: FilePath
dbPath = "/tmp/tmpdb"

getDB :: (FilePath -> HashDB.HashFunction -> HashDB.Size -> t) -> t
getDB f = f dbPath HashDB.stdHash $ HashDB.mkSize 10000

pad :: Int -> SBS.ByteString -> SBS.ByteString
pad n bs = SBS.append bs $ SBS.replicate (n - SBS.length bs) 0

mkKey :: SBS.ByteString -> HashDB.Key
mkKey = HashDB.mkKey . pad 16

main :: IO ()
main = do
  db <-
    getDB HashDB.openDatabase
    `Exc.catch` \(Exc.SomeException _) -> getDB HashDB.createDatabase
  HashDB.writeKey db (mkKey "a") "a!"
  HashDB.writeKey db (mkKey "b") "b!"
  print =<< HashDB.readKey db (mkKey "a")
  print =<< HashDB.readKey db (mkKey "b")
