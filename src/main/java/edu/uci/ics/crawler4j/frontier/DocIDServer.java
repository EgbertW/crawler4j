/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.crawler4j.frontier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.util.IterateAction;
import edu.uci.ics.crawler4j.util.Processor;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */

public class DocIDServer extends Configurable {
  private static final Logger logger = LoggerFactory.getLogger(DocIDServer.class);

  private Database docIDsDB;
  private static final String DATABASE_NAME = "DocIDs";

  private final Object mutex = new Object();

  private long lastDocID;

  public DocIDServer(Environment env, CrawlConfig config) {
    super(config);
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setTransactional(config.isResumableCrawling());
    dbConfig.setDeferredWrite(!config.isResumableCrawling());
    lastDocID = 0;
    docIDsDB = env.openDatabase(null, DATABASE_NAME, dbConfig);
    if (config.isResumableCrawling()) {
      long docCount = getDocCount();
      if (docCount > 0) {
        logger.info("Loaded {} URLs that had been detected in previous crawl.", docCount);
        lastDocID = docCount;
      }
    }
  }

  /**
   * Returns the docid of an already seen url.
   *
   * @param url the URL for which the docid is returned.
   * @return the docid of the url if it is seen before. Otherwise -1 is returned.
   */
  public long getDocId(String url) {
    synchronized (mutex) {
      OperationStatus result = null;
      DatabaseEntry value = new DatabaseEntry();
      try {
        DatabaseEntry key = new DatabaseEntry(url.getBytes());
        result = docIDsDB.get(null, key, value, null);

      } catch (Exception e) {
        logger.error("Exception thrown while getting DocID", e);
        return -1;
      }

      if ((result == OperationStatus.SUCCESS) && (value.getData().length > 0)) {
        return Util.byteArray2Long(value.getData());
      }

      return -1;
    }
  }
  
  /**
   * Attempt to get a new doc ID. If the URL was seen before,
   * -1 will be returned. This is a synchronized combination
   * of getNewDocID and getDocId.
   * 
   * @param url The url to get a doc id for
   * @return A new doc id if the URL is unseen, -1 otherwise
   */
  public long getNewUnseenDocID(String url) {
    synchronized (mutex) {
        long docid = getDocId(url);
        if (docid > 0)
          return -1;
        return getNewDocID(url);
    }
  }

  public long getNewDocID(String url) {
    synchronized (mutex) {
      try {
        // Make sure that we have not already assigned a docid for this URL
        long docID = getDocId(url);
        if (docID > 0) {
          return docID;
        }

        ++lastDocID;
        docIDsDB.put(null, new DatabaseEntry(url.getBytes()), new DatabaseEntry(Util.long2ByteArray(lastDocID)));
        return lastDocID;
      } catch (Exception e) {
        logger.error("Exception thrown while getting new DocID", e);
        return -1;
      }
    }
  }

  public void addUrlAndDocId(String url, long docId) throws Exception {
    synchronized (mutex) {
      if (docId <= lastDocID) {
        throw new Exception("Requested doc id: " + docId + " is not larger than: " + lastDocID);
      }

      // Make sure that we have not already assigned a docid for this URL
      long prevDocid = getDocId(url);
      if (prevDocid > 0) {
        if (prevDocid == docId) {
          return;
        }
        throw new Exception("Doc id: " + prevDocid + " is already assigned to URL: " + url);
      }

      docIDsDB.put(null, new DatabaseEntry(url.getBytes()), new DatabaseEntry(Util.long2ByteArray(docId)));
      lastDocID = docId;
    }
  }

  public boolean isSeenBefore(String url) {
    return getDocId(url) != -1;
  }

  public final long getDocCount() {
    try {
      return (long) docIDsDB.count();
    } catch (DatabaseException e) {
      logger.error("Exception thrown while getting DOC Count", e);
      return -1;
    }
  }
  
  public boolean forget(String url) {
    synchronized (mutex) {
      DatabaseEntry key = new DatabaseEntry(url.getBytes());
      DatabaseEntry val = new DatabaseEntry();
      
      if (docIDsDB.get(null, key, val, null) == OperationStatus.NOTFOUND)
        return false;
      
      Cursor c = docIDsDB.openCursor(null, null);
      if (c.getSearchKey(key, val, null) == OperationStatus.NOTFOUND)
        return false;
      
      if (c.delete() == OperationStatus.SUCCESS)
        return true;
      
      throw new RuntimeException("Failed to remove entry for URL " + url);
    }
  }
  
  /** 
   * Iterate over all the docids and hand them one-by-one to a callback,
   * that can then decide whether to remove, return, remove and return or continue
   * with the iteration.
   * 
   * @param processor An object on which the apply function will be called for each element
   * @return The url for which RETURN or REMOVE_AND_RETURN was indicated, or null if that did not happen
   */
  public String iterate(Processor<String, IterateAction> processor) {
    synchronized (mutex) {
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction txn = null; //beginTransaction();
      String url;
      
      try (Cursor cursor = docIDsDB.openCursor(txn, null)) {
        OperationStatus result = cursor.getFirst(key, value,  null);
        while (result == OperationStatus.SUCCESS) {
          url = new String(key.getData());
          IterateAction action = processor.apply(url);
          
          if (action == IterateAction.REMOVE || action == IterateAction.REMOVE_AND_RETURN)
          {
            result = cursor.delete();
            if (result != OperationStatus.SUCCESS)
              throw new RuntimeException("Could not remove element from database");
          }
          
          if (action == IterateAction.RETURN || action == IterateAction.REMOVE_AND_RETURN)
            return url;
          
          result = cursor.getNext(key, value, null);
        }
      } catch (DatabaseException e) {
        logger.error("Database error during iteration: {}", e.getMessage());
        logger.debug("Stacktrace", e);
      }
    }
    
    // The callback did not return RETURN or REMOVE_AND_RETURN for any
    // element, so there is nothing to return
    return null;
  }
  
  /**
   * Remove all elements from the Document ID server. This can be used in order
   * to reset crawling in case of a long-running web crawler where pages actually need
   * to be re-crawled after a period of time.
   * 
   * Running this while pages are enqueued can result in unexpected behavior because
   * references to non-existing DocIDs may then occur.
   */
  public void clear() {
    synchronized (mutex) {
      try {
        Cursor c = docIDsDB.openCursor(null, null);
        DatabaseEntry k = new DatabaseEntry();
        DatabaseEntry v = new DatabaseEntry();
        long count = docIDsDB.count();
        while (c.getNext(k, v, null) == OperationStatus.SUCCESS)
          c.delete();
        
        logger.info("Succesfully removed {} elements from the database", count);
      }
      catch (DatabaseException e) {
        logger.error("Could not remove all elements from DocIdServer: {}. Recreating database instead", e.getMessage());
        logger.trace("Stacktrace: ", e);

        // Get configuration from the current database
        Environment env = docIDsDB.getEnvironment();
        DatabaseConfig dbConfig = docIDsDB.getConfig();
        
        // Close and remove the database
        docIDsDB.close();
        env.removeDatabase(null,  "DocIDs");
        
        // Create/reopen the database with the same configuration
        docIDsDB = env.openDatabase(null, "DocIDs", dbConfig);
        
        logger.info("Succesfully recreated the database");
      }
      lastDocID = 0;
    }
  }

  public void close() {
    try {
      docIDsDB.close();
    } catch (DatabaseException e) {
      logger.error("Exception thrown while closing DocIDServer", e);
    }
  }
}
