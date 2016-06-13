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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IterateAction;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */
public class URLQueue {
  /** Logger */
  public static final Logger log = LoggerFactory.getLogger(URLQueue.class);
  
  /** The BerkeleyDB database storing the URL queue*/
  private final Database urlsDB;
  
  /** The seed counter database on disk */
  private Database seedCountDB = null;
  
  /** The seed counter */
  private final Map<Long, Integer> seedCount = new HashMap<Long, Integer>();
  
  /** The BerkeleyDB environment */
  private final Environment env;
  
  /** The binding to convert WebURLs to bytes and back */
  private final WebURLTupleBinding webURLBinding;

  /** The mutex used for synchronization */
  protected final Object mutex = new Object();
  
  /** The current transaction */
  private Transaction txn = null;
  
  public static class TransactionAbort extends Exception {
    private static final long serialVersionUID = -1248234463257519891L;
    private TransactionAbort() {}
    private TransactionAbort(String msg) { super(msg); }
    private TransactionAbort(Throwable e) { super(e); }
    private TransactionAbort(String msg, Throwable e) {super(msg, e); }
  }
  
  public abstract static class DBTransaction {
    public abstract void run() throws TransactionAbort;
  }
  
  public abstract static class DBVisitor {
    public abstract IterateAction visit(WebURL arg) throws TransactionAbort;
  }

  /**
   * Create the URLQueue, backed by a Berkeley database
   * 
   * @param env The BerkeleyDB environment
   * @param dbName The name of the BDB database
   * @param resumable Whether this database may be reused on a new run.
   * 
   * @throws TransactionAbort Whenever the current database could not be loaded
   */
  public URLQueue(Environment env, String dbName, boolean resumable) throws TransactionAbort {
    this.env = env;
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setTransactional(true);
    dbConfig.setDeferredWrite(false);
    urlsDB = env.openDatabase(null, dbName, dbConfig);
    webURLBinding = new WebURLTupleBinding();
    
    // Load seed count from database
    if (resumable) {
      dbConfig.setSortedDuplicates(false);
      seedCountDB = env.openDatabase(null, dbName + "_seedcount", dbConfig);
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction my_txn = beginTransaction();
      try (Cursor cursor = seedCountDB.openCursor(txn, null)) {
        OperationStatus result = cursor.getFirst(key, value, null);

        while (result == OperationStatus.SUCCESS) {
          if (value.getData().length > 0) {
            Long docid = Util.byteArray2Long(key.getData());
            Integer counterValue = Util.byteArray2Int(value.getData());
            seedCount.put(docid, counterValue);
          }
          result = cursor.getNext(key, value, null);
        }
      } catch (Exception e) {
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);
      }
    } else {
      try {
        iterate(new DBVisitor() {
          @Override
          public IterateAction visit(WebURL arg) {
            return IterateAction.REMOVE;
          }
        });
      } catch (TransactionAbort e) {
        // Could not clear database, this does not bode well
        throw new RuntimeException("Failed to clear out database - check filesystem");
      }
    }
  }

  /**
   * Create a new transaction when the mode is set to resumable.
   * 
   * @return True if a transaction was started (and should be commited, false otherwise)
   */
  protected Transaction beginTransaction() {
    if (txn != null)
      return null;
    
    txn = env.beginTransaction(null, null);
    return txn;
  }

  /**
   * Commit the specified transaction. If it is null,
   * nothing happens.
   * 
   * @param txn The transaction to commit.
   */
  protected void commit(Transaction txn) {
    if (txn != null) {
      if (txn != this.txn)
        throw new RuntimeException("Transaction is commited in incorrect order - " + this.txn + " is open, so " + txn + " cannot be commited.");
      
      txn.commit();
      this.txn = null;
    }
  }

  /**
   * Abort the active transaction. If it is null,
   * nothing happens.
   * 
   * @param cause The reason to abort
   * @throws TransactionAbort Always, in order to propagate the exception
   */
  protected void abort(Throwable cause) throws TransactionAbort {
    if (txn != null) {
      log.error("Aborting transaction: {}", txn);
      txn.abort();
      txn = null;
    }
    throw new TransactionAbort(cause);
  }

  /**
   * Start a new cursor in the given transaction
   * 
   * @param txn The transaction in which to open the cursor
   * @return The opened cursor
   */
  protected Cursor openCursor(Transaction txn) {
    return urlsDB.openCursor(txn, null);
  }

  /**
   * Select *AND* remove the first set of items from the work queue
   * 
   * @param max The maximum number of items to return
   * @return The list of items, limited by max
   * @throws TransactionAbort When the sleepycat database throws an error
   */
  public List<WebURL> shift(int max) throws TransactionAbort {
    synchronized (mutex) {
      List<WebURL> results = new ArrayList<>(max);
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction my_txn = beginTransaction();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getFirst(key, value, null);
        int matches = 0;
        while ((matches < max) && result == OperationStatus.SUCCESS) {
          byte [] data = value.getData();
          cursor.delete();
          if (data.length > 0) {
            WebURL url = webURLBinding.entryToObject(value);
            seedDecrease(url.getSeedDocid());
            results.add(url);
            matches++;
          }
          result = cursor.getNext(key, value, null);
        }
      } catch (DatabaseException e) {
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);
      }
        
      return results;
    }
  }
  
  /**
   * Get the current seed counter value of a seed docid
   * 
   * @param docid The seed docid for which to get the current value
   * @return The current value of the seed counter
   */
  public int getSeedCount(Long docid) {
    synchronized (mutex) {
      return seedCount.containsKey(docid) ? seedCount.get(docid) : 0;
    }
  }
  
  /**
   * Update the seed counter value for a seed docid
   * 
   * @param docid The seed docid for which to set the counter
   * @param value The new value for the counter
   * @throws TransactionAbort When a database failure occured during the operation
   */
  private void setSeedCount(Long docid, Integer value) throws TransactionAbort {
    DatabaseEntry key = new DatabaseEntry(Util.long2ByteArray(docid));
    if (value <= 0) {
      synchronized (mutex) {
        seedCount.remove(docid);
        if (seedCountDB != null) {
          Transaction my_txn = beginTransaction();
          try {
            seedCountDB.delete(txn, key);
          } catch (DatabaseException e) {
            my_txn = null;
            abort(e);
          } finally {
            commit(my_txn);
          }
        }
      }
      return;
    }
      
    synchronized (mutex) {
      seedCount.put(docid, value);
      if (seedCountDB != null) {
        Transaction my_txn = beginTransaction();
        try {
          DatabaseEntry val = new DatabaseEntry(Util.int2ByteArray(value));
          seedCountDB.put(txn, key, val);
        } catch (DatabaseException e) {
          my_txn = null;
          abort(e);
        } finally {
          commit(my_txn);
        }
      }
    }
  }
  
  /**
   * Increase the seed counter by 1
   * 
   * @param docid The seed docid for which to increment the counter
   * @throws TransactionAbort When {@link #seedIncrease(Long, Integer)} throws it
   */
  public void seedIncrease(Long docid) throws TransactionAbort {
    seedIncrease(docid, 1);
  }
  
  /**
   * Increase the seed counter by the specified amount
   * 
   * @param docid The seed docid for which to increase the counter
   * @param amount The amount by which to increase it
   * @throws TransactionAbort When {@link #setSeedCount(Long, Integer)} throws it
   */
  public void seedIncrease(Long docid, Integer amount) throws TransactionAbort {
    synchronized (mutex) {
      setSeedCount(docid, getSeedCount(docid) + amount);
    }
  }
  
  /**
   * Increase the seed counter by 1
   * 
   * @param docid The seed docid for which to decrement the counter
   * @throws TransactionAbort When {@link #seedIncrease(Long, Integer)} throws it
   */
  public void seedDecrease(Long docid) throws TransactionAbort {
    seedIncrease(docid, -1);
  }
  
  /**
   * Reduce the seed counter by the specified amount
   * 
   * @param docid The seed doc id for which to decrease the counter
   * @param amount The amount by which to reduce it
   * @throws TransactionAbort When {@link #seedIncrease(Long, Integer)} throws it
   */
  public void seedDecrease(Long docid, Integer amount) throws TransactionAbort {
    seedIncrease(docid, -amount);
  }

  /**
   * The key that is used for storing URLs determines the order
   * they are crawled. Lower key values results in earlier crawling.
   * Here our keys are 10 bytes. The first byte comes from the URL priority.
   * The second byte comes from depth of crawl at which this URL is first found.
   * The remaining 8 bytes come from the docid of the URL. As a result,
   * URLs with lower priority numbers will be crawled earlier. If priority
   * numbers are the same, those found at lower depths will be crawled earlier.
   * If depth is also equal, those found earlier (therefore, smaller docid) will
   * be crawled earlier.
   * 
   * @param url The WebURL to convert to a Database key
   * @return The 10-byte database key
   */
  public static DatabaseEntry getDatabaseEntryKey(WebURL url) {
    return new DatabaseEntry(url.getKey());
  }

  /**
   * Add a URL to the queue
   * 
   * @param url The URL to add
   * @return True if the URL was added, false if it was already in the queue
   * @throws TransactionAbort When something went wrong and the transaction was aborted
   */
  public boolean put(WebURL url) throws TransactionAbort {
    synchronized (mutex) {
      boolean added = false;
      DatabaseEntry value = new DatabaseEntry();
      webURLBinding.objectToEntry(url, value);
      Transaction my_txn = beginTransaction();
      try { 
        // Check if the key already exists
        DatabaseEntry key = getDatabaseEntryKey(url);
        DatabaseEntry retrieve_value = new DatabaseEntry();
        if (urlsDB.get(txn, key, retrieve_value, null) == OperationStatus.NOTFOUND) {
          urlsDB.put(txn, key, value);
          seedIncrease(url.getSeedDocid());
          added = true;
        }
      } catch (DatabaseException | TransactionAbort e) {
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);
      }
      
      return added;
    }
  }
  
  /**
   * Add a list of URLs to the queue in one batch.
   * 
   * @param urls The list of URLs to add
   * @return The number of URLs added. Duplicates are not added again.
   * @throws TransactionAbort When something went wrong and the transaction was aborted
   */
  public List<WebURL> put(Collection<WebURL> urls) throws TransactionAbort {
    synchronized (mutex) {
      List<WebURL> rejects = new ArrayList<WebURL>();
      Transaction my_txn = beginTransaction();
      try {
        for (WebURL url : urls) {
          DatabaseEntry value = new DatabaseEntry();
          webURLBinding.objectToEntry(url, value);
        
          // Check if the key already exists
          DatabaseEntry key = getDatabaseEntryKey(url);
          DatabaseEntry retrieve_value = new DatabaseEntry();
          if (urlsDB.get(txn, key, retrieve_value, null) == OperationStatus.NOTFOUND) {
            urlsDB.put(txn, key, value);
            seedIncrease(url.getSeedDocid());
          } else {
            rejects.add(url);
          }
        }
      } catch (DatabaseException | TransactionAbort e) {
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);
      }
      
      return rejects;
    }
  }

  /**
   * @return The number of elements on the queue
   */
  public long getLength() {
    return urlsDB.count();
  }

  /**
   * Close the database
   */
  public void close() {
    urlsDB.close();
  }
  
  /**
   * Iterate over the database, passing each value to the apply method of the
   * provided callback object. Depending on the return value of the apply method,
   * the iteration continues or returns the found object, optionally deleting
   * the object.
   * 
   * @param callback The callback object that gets all the elements
   * @return A WebURL for which REMOVE_AND_RETURN or RETURN was returned, or null if that did not happen.
   * @throws TransactionAbort Thrown when iteration was aborted and the transaction was rolled back.
   */
  public WebURL iterate(DBVisitor callback) throws TransactionAbort {
    synchronized (mutex) {
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction my_txn = beginTransaction();
      WebURL url;
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getFirst(key, value,  null);
        while (result == OperationStatus.SUCCESS) {
          url = webURLBinding.entryToObject(value);
          IterateAction action = callback.visit(url);
          
          if (action == IterateAction.REMOVE || action == IterateAction.REMOVE_AND_RETURN)
          {
            result = cursor.delete();
            if (result != OperationStatus.SUCCESS)
              throw new RuntimeException("Could not remove element from database");
            
            seedDecrease(url.getSeedDocid());
          }
          
          if (action == IterateAction.RETURN || action == IterateAction.REMOVE_AND_RETURN)
            return url;
          
          result = cursor.getNext(key, value, null);
        }
      } catch (Exception e) {
        log.error("Exception occured while iterating over database", e);
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);
      }
    }
    
    // The callback did not return RETURN or REMOVE_AND_RETURN for any
    // element, so there is nothing to return
    return null;
  }
  
  /**
   * @return A list of all URLs in the database.
   */
  public List<WebURL> getDump() {
    final List<WebURL> list = new ArrayList<WebURL>();
    try {
      iterate(new DBVisitor() {
        public IterateAction visit(WebURL url) throws TransactionAbort {
          list.add(url);
          return IterateAction.CONTINUE;
        }
      });
    } catch (TransactionAbort e) {
      log.error("Transaction aborted while generating dump", e);
    }
    
    return list;
  }
  
  /**
   * Update a set of URLs in the database in one transaction.
   * 
   * @param urls The URLS to update
   * @throws TransactionAbort Whenever something goes wrong
   * @see #update(WebURL)
   */
  public void update(Collection<WebURL> urls) throws TransactionAbort {
    log.trace("Updating set of {} urls", urls.size());
    Transaction my_txn = beginTransaction();
    
    try (Cursor cursor = openCursor(txn)) {
      for (WebURL url : urls) {
        log.trace("Multi-update: updating {}", url);
        DatabaseEntry key = getDatabaseEntryKey(url);
        DatabaseEntry value = new DatabaseEntry();
        DatabaseEntry new_value = new DatabaseEntry();
        webURLBinding.objectToEntry(url, new_value);
        
        OperationStatus result = cursor.getSearchKey(key, value, null);
        if (result != OperationStatus.SUCCESS) {
          abort(new RuntimeException("Failed to find URL to update"));
        }
        
        cursor.putCurrent(new_value);
      }
    } catch (DatabaseException | TransactionAbort e) {
      my_txn = null; // Make sure the transaction is not committed
      abort(e);
    } finally {
      commit(my_txn);
    }
  }
  
  /**
   * Update a URL in the database. It is assumed that the key information
   * nor the seed status changed. If it has, a remove/put combination should be 
   * used in stead. Seed counters are therefore not updated.
   * 
   * @param url The URL to update
   * @return True when the update succeeded, false otherwise
   * @throws TransactionAbort When something went wrong in the database
   */
  public boolean update(WebURL url) throws TransactionAbort {
    log.trace("Updating {}", url);
    Transaction my_txn = beginTransaction();
    DatabaseEntry key = getDatabaseEntryKey(url);
    DatabaseEntry value = new DatabaseEntry();
    DatabaseEntry new_value = new DatabaseEntry();
    webURLBinding.objectToEntry(url, new_value);
    
    try (Cursor cursor = openCursor(txn)) {
      OperationStatus result = cursor.getSearchKey(key, value, null);
      if (result == OperationStatus.SUCCESS) {
        cursor.putCurrent(new_value);
        return true;
      }
      log.error("Could not find URL to update using key: {}", url.getKey());
    } catch (DatabaseException e) {
      my_txn = null;
      abort(e);
    } finally {
      commit(my_txn);
    }
    
    // Not found
    return false;
  }
  
  public WebURL get(byte [] key) throws TransactionAbort {
    if (key == null)
      return null;
    
    DatabaseEntry dbkey = new DatabaseEntry();
    dbkey.setData(key);
    DatabaseEntry value = new DatabaseEntry();
    WebURL url;
    
    Transaction my_txn = beginTransaction();
    try (Cursor cursor = openCursor(txn)) {
      OperationStatus result = cursor.getSearchKey(dbkey, value, null);
      
      if (result == OperationStatus.SUCCESS) {
        url = webURLBinding.entryToObject(value);
        return url;
      }
    } catch (DatabaseException e) {
      my_txn = null;
      abort(e);
    } finally {
      commit(my_txn);
    }
    
    // Not found
    return null;
  }

  /**
   * Remove all offspring of the given seed docid
   * 
   * @param seed_doc_id The seed for which to remove the offspring
   * @return The number of elements removed
   * @throws TransactionAbort Whenever the any error occurs in the database
   */
  public int removeOffspring(final long seed_doc_id) throws TransactionAbort {
    final Util.Reference<Integer> num_removed = new Util.Reference<Integer>(0);
    final Util.Reference<Integer> to_remove = new Util.Reference<Integer>(seedCount.get(seed_doc_id));
    if (to_remove.val == null)
      return 0;
    
    Transaction my_txn = beginTransaction();
    try {
      iterate(new DBVisitor() {
        @Override
        public IterateAction visit(WebURL url) {
          if (url.getSeedDocid() == seed_doc_id) {
            if (++num_removed.val == to_remove.val)
              return IterateAction.REMOVE_AND_RETURN;
      
            return IterateAction.REMOVE;
          }
          return IterateAction.CONTINUE;
        }
      });
    } catch (TransactionAbort e) {
      my_txn = null;
      abort(e);
    } finally {
      commit(my_txn);
    }
    
    return num_removed.get();
  }
  
  public void transaction(DBTransaction r) throws TransactionAbort {
    Transaction my_txn = beginTransaction();
    try {
      r.run();
    } catch (Exception e) {
      log.error("Reverting transaction after exception", e);
      my_txn = null;
      abort(e);
    } finally {
      commit(my_txn);
    }
  }
  
  /**
   * Remove a specific WebURL from the queue
   * 
   * @param webUrl The URL to remove
   * @throws TransactionAbort If the element could not be removed
   */
  public void removeURL(WebURL webUrl) throws TransactionAbort {
    Transaction my_txn = beginTransaction();
    synchronized (mutex) {
      boolean removed = false;
      DatabaseEntry key = getDatabaseEntryKey(webUrl);
      DatabaseEntry value = new DatabaseEntry();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getSearchKey(key, value, null);

        if (result != OperationStatus.SUCCESS) 
          throw new TransactionAbort("Element does not exist in queue");
        
        result = cursor.delete();
        if (result != OperationStatus.SUCCESS)
          throw new TransactionAbort("Element could not be removed");
        
        if (webUrl.getSeedDocid() >= 0)
          seedDecrease(webUrl.getSeedDocid());
        removed = true;
      } catch (Exception e) {
        my_txn = null;
        abort(e);
      } finally {
        commit(my_txn);

        if (!removed)
          throw new RuntimeException("URL " + webUrl + " was not present in database");
      }
    }
  }
}
