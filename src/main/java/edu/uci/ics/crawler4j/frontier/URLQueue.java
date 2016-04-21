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
import edu.uci.ics.crawler4j.util.Processor;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */
public class URLQueue {
  /** The BerkeleyDB database storing the URL queue*/
  private final Database urlsDB;
  
  /** The seed counter database on disk */
  private Database seedCountDB = null;
  
  /** The seed counter */
  private final Map<Long, Integer> seedCount = new HashMap<Long, Integer>();
  
  /** The BerkeleyDB environment */
  private final Environment env;
  
  /** Whether crawling is resumable */
  private final boolean resumable;

  /** The binding to convert WebURLs to bytes and back */
  private final WebURLTupleBinding webURLBinding;

  /** The mutex used for synchronization */
  protected final Object mutex = new Object();
  
  /** The current transaction */
  private Transaction txn = null;

  /**
   * Create the URLQueue, backed by a Berkeley database
   * 
   * @param env The BerkeleyDB environment
   * @param dbName The name of the BDB database
   * @param resumable Whether this database may be reused on a new run.
   */
  public URLQueue(Environment env, String dbName, boolean resumable) {
    this.env = env;
    this.resumable = resumable;
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(true);
    dbConfig.setTransactional(resumable);
    dbConfig.setDeferredWrite(!resumable);
    urlsDB = env.openDatabase(null, dbName, dbConfig);
    webURLBinding = new WebURLTupleBinding();
    
    // Load seed count from database
    if (resumable) {
      dbConfig.setSortedDuplicates(false);
      seedCountDB = env.openDatabase(null, dbName + "_seedcount", dbConfig);
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      boolean shouldCommit = beginTransaction();
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
      } finally {
        if (shouldCommit)
          commit();
      }
    }
  }

  /**
   * Create a new transaction when the mode is set to resumable.
   * 
   * @return True if a transaction was started (and should be commited, false otherwise)
   */
  protected boolean beginTransaction() {
    if (txn != null || !resumable)
      return false;
    
    txn = env.beginTransaction(null, null);
    return true;
  }

  /**
   * Commit the specified transaction. If it is null,
   * nothing happens.
   * 
   */
  protected void commit() {
    if (txn != null) {
      txn.commit();
      txn = null;
    }
  }

  /**
   * Abort the specified transaction. If it is null,
   * nothing happens.
   * 
   */
  protected void abort() {
    if (txn != null) {
      txn.abort();
      txn = null;
    }
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
   * @throws DatabaseException When the sleepycat database throws an error
   */
  public List<WebURL> shift(int max) throws DatabaseException {
    synchronized (mutex) {
      List<WebURL> results = new ArrayList<>(max);
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      boolean shouldCommit = beginTransaction();
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
        abort();
        txn = null;
        throw e;
      } finally {
        if (shouldCommit)
          commit();
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
   */
  private void setSeedCount(Long docid, Integer value) {
    DatabaseEntry key = new DatabaseEntry(Util.long2ByteArray(docid));
    if (value <= 0) {
      synchronized (mutex) {
        seedCount.remove(docid);
        if (seedCountDB != null) {
          Transaction txn = env.beginTransaction(null, null);
          seedCountDB.delete(txn, key);
          txn.commit();
        }
      }
      return;
    }
      
    synchronized (mutex) {
      seedCount.put(docid, value);
      if (seedCountDB != null) {
        DatabaseEntry val = new DatabaseEntry(Util.int2ByteArray(value));
        Transaction txn = env.beginTransaction(null, null);
        seedCountDB.put(txn, key, val);
        txn.commit();
      }
    }
  }
  
  /**
   * Increase the seed counter by 1
   * 
   * @param docid The seed docid for which to increment the counter
   */
  public void seedIncrease(Long docid) {
    seedIncrease(docid, 1);
  }
  
  /**
   * Increase the seed counter by the specified amount
   * 
   * @param docid The seed docid for which to increase the counter
   * @param amount The amount by which to increase it
   */
  public void seedIncrease(Long docid, Integer amount) {
    synchronized (mutex) {
      setSeedCount(docid, getSeedCount(docid) + amount);
    }
  }
  
  /**
   * Increase the seed counter by 1
   * 
   * @param docid The seed docid for which to decrement the counter
   */
  public void seedDecrease(Long docid) {
    seedIncrease(docid, -1);
  }
  
  /**
   * Reduce the seed counter by the specified amount
   * 
   * @param docid The seed doc id for which to decrease the counter
   * @param amount The amount by which to reduce it
   */
  public void seedDecrease(Long docid, Integer amount) {
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
   */
  public boolean put(WebURL url) {
    synchronized (mutex) {
      boolean added = false;
      DatabaseEntry value = new DatabaseEntry();
      webURLBinding.objectToEntry(url, value);
      boolean shouldCommit = beginTransaction();
      // Check if the key already exists
      DatabaseEntry key = getDatabaseEntryKey(url);
      DatabaseEntry retrieve_value = new DatabaseEntry();
      if (urlsDB.get(txn, key, retrieve_value, null) == OperationStatus.NOTFOUND) {
        urlsDB.put(txn, key, value);
        seedIncrease(url.getSeedDocid());
        added = true;
      }
      if (shouldCommit)
        commit();
      
      return added;
    }
  }
  
  /**
   * Add a list of URLs to the queue in one batch.
   * 
   * @param urls The list of URLs to add
   * @return The number of URLs added. Duplicates are not added again.
   */
  public List<WebURL> put(Collection<WebURL> urls) {
    synchronized (mutex) {
      List<WebURL> rejects = new ArrayList<WebURL>();
      boolean shouldCommit = beginTransaction();
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
      
      if (shouldCommit)
        commit();
      
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
   */
  public WebURL iterate(Processor<WebURL, IterateAction> callback) {
    synchronized (mutex) {
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      boolean shouldCommit = beginTransaction();
      WebURL url;
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getFirst(key, value,  null);
        while (result == OperationStatus.SUCCESS) {
          url = webURLBinding.entryToObject(value);
          IterateAction action = callback.apply(url);
          
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
      } catch (DatabaseException e) {
        abort();
      } finally {
        if (shouldCommit)
          commit();
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
    iterate(new Processor<WebURL, IterateAction>() {
      public IterateAction apply(WebURL url) {
        list.add(url);
        return IterateAction.CONTINUE;
      }
    });
    
    return list;
  }
  
  /**
   * Update a set of URLs in the database in one transaction.
   * 
   * @param urls The URLS to update
   * @throws DatabaseException Whenever something goes wrong
   * @see #update(WebURL)
   */
  public void update(Collection<WebURL> urls) throws DatabaseException {
    boolean shouldCommit = beginTransaction();
    
    try (Cursor cursor = openCursor(txn)) {
      for (WebURL url : urls) {
        DatabaseEntry key = getDatabaseEntryKey(url);
        DatabaseEntry value = new DatabaseEntry();
        DatabaseEntry new_value = new DatabaseEntry();
        webURLBinding.objectToEntry(url, new_value);
        BerkeleyDBQueue.logger.info("Multi-Updating key: {} - {}", url, url.getKey());
        
        OperationStatus result = cursor.getSearchKey(key, value, null);
        if (result != OperationStatus.SUCCESS)
          throw new RuntimeException("Failed to find URL to update");
        
        cursor.putCurrent(new_value);
      }
    } finally {
      if (shouldCommit)
        commit();
    }
  }
  
  /**
   * Update a URL in the database. It is assumed that the key information
   * nor the seed status changed. If it has, a remove/put combination should be 
   * used in stead. Seed counters are therefore not updated.
   * 
   * @param url The URL to update
   * @return True when the update succeeded, false otherwise
   * @throws DatabaseException When something went wrong in the database
   */
  public boolean update(WebURL url) throws DatabaseException {
    boolean shouldCommit = beginTransaction();
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
    } finally {
      if (shouldCommit)
        commit();
    }
    
    // Not found
    return false;
  }
  
  public WebURL get(byte [] key) throws DatabaseException {
    if (key == null)
      return null;
    
    DatabaseEntry dbkey = new DatabaseEntry();
    dbkey.setData(key);
    DatabaseEntry value = new DatabaseEntry();
    WebURL url;
    
    boolean shouldCommit = beginTransaction();
    try (Cursor cursor = openCursor(txn)) {
      OperationStatus result = cursor.getSearchKey(dbkey, value, null);
      
      if (result == OperationStatus.SUCCESS) {
        url = webURLBinding.entryToObject(value);
        return url;
      }
      
      // Not found
      return null;
    } finally {
      if (shouldCommit)
        commit();
    }
  }

  /**
   * Remove all offspring of the given seed docid
   * 
   * @param seed_doc_id The seed for which to remove the offspring
   * @return The number of elements removed
   */
  public int removeOffspring(final long seed_doc_id) {
    final Util.Reference<Integer> num_removed = new Util.Reference<Integer>(0);
    iterate(new Processor<WebURL, IterateAction>() {
      @Override
      public IterateAction apply(WebURL url) {
        if (url.getSeedDocid() == seed_doc_id) {
          num_removed.assign(num_removed.get() + 1);
          return IterateAction.REMOVE;
        }
        return IterateAction.CONTINUE;
      }
    });
    
    return num_removed.get();
  }
  
  /**
   * Remove a specific WebURL from the queue
   * 
   * @param webUrl The URL to remove
   * @return True if the element was removed, false otherwise.
   */
  public boolean removeURL(WebURL webUrl) {
    synchronized (mutex) {
      boolean removed = false;
      DatabaseEntry key = getDatabaseEntryKey(webUrl);
      DatabaseEntry value = new DatabaseEntry();
      boolean shouldCommit = beginTransaction();
      try (Cursor cursor = openCursor(txn)) {
        OperationStatus result = cursor.getSearchKey(key, value, null);

        if (result == OperationStatus.SUCCESS) {
          result = cursor.delete();
          if (result == OperationStatus.SUCCESS) {
            removed = true;
            return true;
          }
        }
      } catch (DatabaseException e) {
        abort();
      } finally {
        if (shouldCommit)
          commit();

        if (removed && webUrl.getSeedDocid() >= 0)
          seedDecrease(webUrl.getSeedDocid());
        else
          throw new RuntimeException("URL " + webUrl.getURL() + " was not present in list of processed pages. " + (removed ? "true" : "false") + " seeddocid: " + webUrl.getSeedDocid());
      }
    }
    return false;
  }
}
