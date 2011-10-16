{-# LANGUAGE FlexibleContexts, FlexibleInstances, FunctionalDependencies, MultiParamTypeClasses, RankNTypes, ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wall -Wwarn -fno-warn-name-shadowing #-}
module Happstack.Search
    ( Index(Index, documentMap, rebuildIndex, dirtyDocuments)
    , HasKeywords(keywords, docid)
    , addKeywords
    , Indexer'(..)
    , MonadSearchIndex(..)
    , startIndexer
    , dirtyThis
    , touch
    , search
    , stopIndexer
    ) where

import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TVar
import Control.Monad (forever)
import Control.Monad.State (StateT, get)
import Control.Monad.STM (atomically)
import Control.Monad.Trans(MonadIO(liftIO), lift)
import qualified Data.List as List
import Data.Map (Map)
import Data.Maybe (fromMaybe, catMaybes)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text as T
import HSP (XMLGenT)
import Web.Routes (MonadRoute(..))

{-

Search requirement:

 - building the search index is expensive, so we do not want to do it for each search
 - when the documents/assertions are updated, we need to update the index
 - we do not want the user to have to wait for the indexing to happen when they add an document/assertion
 - when doing a query, it is ok if they use a slightly out of date index
 - we do not want to build the map lazily, because that can make a search take a while

search :: [T.Text] -> m [(Int, AssertionId)]
update :: Assert -> Assertion -> m ()

the update should return immediately, but start the indexing in the background.
search should use the most current update to date index.

We only want one update to run at a time. specifically, the most
recent update. If an update is in progress, we should just kill it and
start the new one. Though under contention that could lead to the index never being updated. 

-}

-- | An Index for a type i, includes a map like one produced by
-- addKeywords, a flag saying the index needs to be rebuilt, and a set
-- of dirty document ids.
data Ord i => Index i
    = Index { documentMap :: Map T.Text [(Int, i)]
            , rebuildIndex :: Bool
            , dirtyDocuments :: Set.Set i }

-- | Anything we want to index must be an instance of this class.
class Ord docid => HasKeywords document docid | document -> docid where
    keywords :: document -> [(Int, T.Text)]
    -- ^ Return a list of (frequency, keyword) pairs for all the keywords we can find.
    docid :: document -> docid
    -- ^ Return the identifier for this document.

-- | A function that adds the keywords from a document into a Map.
addKeywords :: HasKeywords document docid => document -> Map T.Text [(Int, docid)] -> Map T.Text [(Int, docid)]
addKeywords document wordMap =
    foldr addKeyword wordMap (keywords document)
    where addKeyword (frequency, wrd) wordMap =
              Map.insertWith (\ [v] old -> List.insert v old) wrd [(frequency, docid document)] wordMap

-- | This controls the thread which does indexing in parallel to the
-- running server.
data Indexer' indexes =
    Indexer { threadId :: ThreadId
            , index    :: TVar indexes
            , dirty    :: MVar ()
            }

-- | A State monad which contains the search indexer thread.
class Monad m => MonadSearchIndex m indexes where
    askIndexer :: m (Maybe (Indexer' indexes))

instance MonadSearchIndex m indexes => MonadSearchIndex (XMLGenT m) indexes where
    askIndexer = lift askIndexer

-- | MonadSearchIndex is a state monad maybe containing an Indexer.
-- The indexer is optional so we can turn off indexing when necessary.
instance Monad m => MonadSearchIndex (StateT (Maybe (Indexer' indexes)) m) indexes where
    askIndexer = get

-- | Generate the initial index from the database and start the search
-- indexing thread.
startIndexer :: IO database -> (database -> indexes -> indexes) -> indexes -> IO (Maybe (Indexer' indexes))
startIndexer retrieve generate indexes =
    do indexTVar <- atomically $ newTVar indexes
       dirtyMV <- newMVar ()
       tid   <- forkIO $ forever $
                do takeMVar dirtyMV
                   -- liftIO (logM "SeeReason.Search" WARNING "Search index update starting")
                   -- |Invoke the AllReason method with a dummy indexer value so
                   -- we can retrieve the database and initialize the SearchIndex.
                   sr <- retrieve
                   atomically $ readTVar indexTVar >>= \ idx ->
                                let idx' = generate sr idx in
                                writeTVar indexTVar idx'
                   -- liftIO (logM "SeeReason.Search" WARNING "Search index update finished")
                   return ()
       return (Just (Indexer tid indexTVar dirtyMV))

-- | Mark a document id as dirty.
dirtyThis :: Ord i => i -> Index i -> Index i
dirtyThis i idx = idx {dirtyDocuments = Set.insert i (dirtyDocuments idx)}

-- | Modify the search index using f and mark it as dirty.
touch :: (MonadIO m, MonadSearchIndex m indexes) => (indexes -> indexes) -> m ()
touch f =
    askIndexer >>= touch'
    where
      touch' Nothing = return ()
      touch' indexer@(Just (Indexer _ indexTVar _)) =
          do liftIO $ atomically ( readTVar indexTVar >>= \ idx ->
                                   let idx' = f idx in
                                   writeTVar indexTVar idx' )
             dirtyIndex' indexer

      dirtyIndex' :: (MonadIO m) => (Maybe (Indexer' a)) -> m ()
      dirtyIndex' Nothing = return ()
      dirtyIndex' (Just indexer) =
          do _ <- liftIO $ tryPutMVar (dirty indexer) ()
             -- liftIO (logM "SeeReason.Search" INFO "dirtyIndex")
             return ()

-- | Search for a set of keywords in the map returned by the accessor.
search :: forall m ident indexes. (MonadRoute m, MonadIO m, MonadSearchIndex m indexes, Eq ident) =>
          (indexes -> Map T.Text [(Int, ident)]) -> [T.Text] -> m [(Int, ident)]
search accessor keywords =
    askIndexer >>= maybe (error "No search index") search'
    where
      search' indexer =
          liftIO $ do (idx{-@(Indexes {})-}) <- atomically $ readTVar (index indexer)
                      let results = map (\kw -> fromMaybe [] $ Map.lookup (T.toLower kw) (accessor idx)) keywords :: [[(Int, ident)]]
                          combined = intersectWith_ (\ (c1, aid1) (c2, aid2) -> 
                                                         if aid1 == aid2 then
                                                             Just (c1+c2, aid1)
                                                         else
                                                             Nothing) results
                      return combined

      intersectWith :: (a -> b -> Maybe c) -> [a] -> [b] -> [c]
      intersectWith eq xs ys = concat [ catMaybes (map (eq x) ys) | x <- xs ]

      intersectWith_ :: (a -> a -> Maybe a) -> [[a]] -> [a]
      intersectWith_ _ [] = []
      intersectWith_ eq xs = foldr1 (intersectWith eq) xs

-- | Kill the search indexing thread.
stopIndexer :: Maybe (Indexer' a) -> IO ()
stopIndexer indexer = maybe (return ()) (killThread . threadId) indexer
