{-# OPTIONS -Wall #-}
{-# LANGUAGE OverloadedStrings #-}

import Control.Applicative ((<$>))
import Control.Monad (replicateM)
import Data.ByteString.Char8 ()
import System.Random (Random, randomIO)
import qualified Control.Exception as Exc
import qualified Criterion.Main as CM
import qualified Data.ByteString as SBS
import qualified Database.KeyValueHash as HashDB

dbPath :: FilePath
dbPath = "/tmp/tmpdb"

sRandomIO :: Random r => IO r
sRandomIO = randomIO >>= Exc.evaluate

randSBS :: Int -> IO HashDB.Key
randSBS len = SBS.pack <$> replicateM len sRandomIO

rand :: IO ()
rand = do
  _ <- randSBS 16
  return ()

writeAndRead :: HashDB.Database -> IO ()
writeAndRead db = do
  key <- randSBS 16
  HashDB.writeKey db key "a!"
  _ <- HashDB.readKey db key
  return ()

writeOnly :: HashDB.Database -> IO ()
writeOnly db = do
  key <- randSBS 16
  HashDB.writeKey db key "a!"

writeToStaticKey :: HashDB.Database -> IO ()
writeToStaticKey db = do
  HashDB.writeKey db "a" "a!"

writeDynamicToStaticKey :: HashDB.Database -> IO ()
writeDynamicToStaticKey db = do
  len <- (`mod` 10) <$> sRandomIO
  val <- randSBS (len * 100)
  HashDB.writeKey db "a" val

useDB ::
  (FilePath
   -> HashDB.HashFunction
   -> HashDB.Size
   -> IO HashDB.Database)
  -> IO HashDB.Database
useDB f = f dbPath HashDB.stdHash (HashDB.mkSize 10000)

withDB :: (HashDB.Database -> IO c) -> IO c
withDB =
  Exc.bracket
  (useDB HashDB.openDatabase
   `Exc.catch` \(Exc.SomeException _) ->
   useDB HashDB.createDatabase)
  HashDB.closeDatabase

main :: IO ()
main =
  withDB $ \db ->
    CM.defaultMain
    [ CM.bgroup "calibrate"
      [ CM.bench "rand SBS16" rand
      ]
    , CM.bgroup "DB"
      [ CM.bench "write and read" $ writeAndRead db
      , CM.bench "write only" $ writeOnly db
      , CM.bench "write to static key" $ writeToStaticKey db
      , CM.bench "write dynamic to static key" $ writeDynamicToStaticKey db
      ]
    ]
