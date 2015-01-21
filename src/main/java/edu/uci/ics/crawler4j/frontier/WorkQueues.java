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

import com.sleepycat.je.*;

import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Yasser Ganjisaffar <lastname at gmail dot com>
 */
public class WorkQueues {

  protected Database urlsDB = null;
  protected Database seedCountDB = null;
  protected HashMap<Integer, Integer> seedCount = new HashMap<Integer, Integer>();
  protected Environment env;

  protected boolean resumable;

  protected WebURLTupleBinding webURLBinding;

  protected final Object mutex = new Object();

  public WorkQueues(Environment env, String dbName, boolean resumable) throws DatabaseException {
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
      OperationStatus result;
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction tnx = env.beginTransaction(null, null);
      Cursor cursor = seedCountDB.openCursor(tnx, null);
      result = cursor.getFirst(key, value, null);

      while (result == OperationStatus.SUCCESS) {
        if (value.getData().length > 0) {
          Integer docid = Util.byteArray2Int(key.getData());
          Integer counterValue = Util.byteArray2Int(value.getData());
          seedCount.put(docid, counterValue);
        }
        result = cursor.getNext(key, value, null);
      }
      cursor.close();
      tnx.commit();
    }
  }
  
  /**
   * Select *AND* remove the first set of items from the work queue
   * 
   * @param max The maximum number of items to return
   * @return The list of items, limited by max
   * @throws DatabaseException
   */
  public List<WebURL> shift(int max) throws DatabaseException {
    synchronized (mutex) {
      int matches = 0;
      List<WebURL> results = new ArrayList<>(max);

      Cursor cursor = null;
      OperationStatus result;
      DatabaseEntry key = new DatabaseEntry();
      DatabaseEntry value = new DatabaseEntry();
      Transaction txn;
      if (resumable) {
        txn = env.beginTransaction(null, null);
      } else {
        txn = null;
      }
      try {
        cursor = urlsDB.openCursor(txn, null);
        result = cursor.getFirst(key, value, null);

        while (matches < max && result == OperationStatus.SUCCESS) {
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
        if (txn != null) {
          txn.abort();
          txn = null;
        }
        throw e;
      } finally {
        if (cursor != null) {
          cursor.close();
        }
        if (txn != null) {
          txn.commit();
        }
      }
        
      return results;
    }
  }
  
  public int getSeedCount(Integer docid) {
      synchronized (mutex) {
          return seedCount.containsKey(docid) ? seedCount.get(docid) : 0;
      }
  }
  
  private void setSeedCount(Integer docid, Integer value) {
      DatabaseEntry key = new DatabaseEntry(Util.int2ByteArray(docid));
      if (value <= 0)
      {
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
  
  public void seedIncrease(Integer docid) {
      seedIncrease(docid, 1);
  }
  
  public void seedIncrease(Integer docid, Integer amount) {
      synchronized (mutex) {
          setSeedCount(docid, getSeedCount(docid) + amount);
      }
  }
  
  public void seedDecrease(Integer docid) {
      seedIncrease(docid, -1);
  }
  
  public void seedDecrease(Integer docid, Integer amount) {
      seedIncrease(docid, -amount);
  }

  /*
   * The key that is used for storing URLs determines the order
   * they are crawled. Lower key values results in earlier crawling.
   * Here our keys are 6 bytes. The first byte comes from the URL priority.
   * The second byte comes from depth of crawl at which this URL is first found.
   * The rest of the 4 bytes come from the docid of the URL. As a result,
   * URLs with lower priority numbers will be crawled earlier. If priority
   * numbers are the same, those found at lower depths will be crawled earlier.
   * If depth is also equal, those found earlier (therefore, smaller docid) will
   * be crawled earlier.
   */
  protected DatabaseEntry getDatabaseEntryKey(WebURL url) {
    byte[] keyData = new byte[6];
    
    // Because the ordering is done strictly binary, negative values will come last, because
    // their binary representation starts with the MSB at 1. In order to fix this, we'll have
    // to add the minimum value to become 0. This means that the maximum number will become
    // out of range in Byte-value, but the integer value is nicely converted down to the actual
    // binary representation that is useful here.
    byte binary_priority = (byte)(url.getPriority() - Byte.MIN_VALUE);
    keyData[0] = binary_priority;
    keyData[1] = (url.getDepth() > Byte.MAX_VALUE ? Byte.MAX_VALUE : (byte) url.getDepth());
    Util.putIntInByteArray(url.getDocid(), keyData, 2);
    return new DatabaseEntry(keyData);
  }

  public boolean put(WebURL url) throws DatabaseException {
    synchronized (mutex) {
      boolean added = false;
      DatabaseEntry value = new DatabaseEntry();
      webURLBinding.objectToEntry(url, value);
      Transaction txn;
      if (resumable) {
        txn = env.beginTransaction(null, null);
      } else {
        txn = null;
      }
      // Check if the key already exists
      DatabaseEntry key = getDatabaseEntryKey(url);
      DatabaseEntry retrieve_value = new DatabaseEntry();
      if (urlsDB.get(txn, key, retrieve_value, null) == OperationStatus.NOTFOUND) {
        urlsDB.put(txn, key, value);
        seedIncrease(url.getSeedDocid());
        added = true;
      }
      if (resumable) {
        if (txn != null) {
          txn.commit();
        }
      }
      return added;
    }
  }

  public long getLength() {
    try {
      return urlsDB.count();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return -1;
  }

  public void sync() {
    if (resumable) {
      return;
    }
    if (urlsDB == null) {
      return;
    }
    try {
      urlsDB.sync();
      if (seedCountDB != null)
        seedCountDB.sync();
    } catch (DatabaseException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    try {
      urlsDB.close();
      if (seedCountDB != null)
        seedCountDB.close();
    } catch (DatabaseException e) {
      e.printStackTrace();
    }
  }
}