{-# OPTIONS -Wall #-}
{-# LANGUAGE OverloadedStrings #-}
import Control.Monad (replicateM_)
import Data.ByteString.Char8 ()
import qualified Control.Exception as Exc
import qualified Database.KeyValueHash as HashDB

dbPath :: FilePath
dbPath = "/tmp/tmpdb"

test :: HashDB.Database -> IO ()
test db = do
  HashDB.writeKey db "a" "a!"
  HashDB.writeKey db "b" "b!"
  print =<< HashDB.readKey db "a"
  print =<< HashDB.readKey db "b"
  print =<< HashDB.readKey db "c"
  print =<< HashDB.readKey db "d"
  replicateM_ 10000 $ do
    HashDB.writeKey db "c" "ghi"
    HashDB.writeKey db "d" "ghi"
    HashDB.writeKey db "c" "abcef"
    HashDB.writeKey db "d" "abcef"

useDB ::
  (FilePath
   -> HashDB.HashFunction
   -> HashDB.Size
   -> (HashDB.Database -> IO ())
   -> IO ())
  -> IO ()
useDB f = f dbPath HashDB.stdHash (HashDB.mkSize 10000) test

main :: IO ()
main =
  useDB HashDB.withOpenDatabase
    `Exc.catch` \(Exc.SomeException _) ->
    useDB HashDB.withCreateDatabase
