{-# OPTIONS -Wall #-}
{-# LANGUAGE OverloadedStrings #-}
import Data.ByteString.Char8 ()
import qualified Control.Exception as Exc
import qualified Database.Hash as HashDB

dbPath :: FilePath
dbPath = "/tmp/tmpdb"

getDB :: (FilePath -> HashDB.HashFunction -> HashDB.Size -> t) -> t
getDB f = f dbPath HashDB.stdHash $ HashDB.mkSize 10000

main :: IO ()
main = do
  db <-
    getDB HashDB.openDatabase
    `Exc.catch` \(Exc.SomeException _) -> getDB HashDB.createDatabase
  HashDB.writeKey db "a" "a!"
  HashDB.writeKey db "b" "b!"
  print =<< HashDB.readKey db "a"
  print =<< HashDB.readKey db "b"
