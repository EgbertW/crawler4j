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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar
 */

public class Frontier extends Configurable {
  protected static final Logger logger = LoggerFactory.getLogger(Frontier.class);
  
  private static final String DATABASE_NAME = "PendingURLsDB";
  private static final int IN_PROCESS_RESCHEDULE_BATCH_SIZE = 100;

  /** Identifier for the InProgress queue: pages in progress by a thread */
  public static final int IN_PROGRESS_QUEUE = 1;
  /** Identifier for the WorkQueue: pages not yet claimed by any thread */
  public static final int WORK_QUEUE = 2;
  /** convenience identifier for both queues: IN_PROGRESS_QUEUE | WORK_QUEUE */
  public static final int BOTH_QUEUES = IN_PROGRESS_QUEUE | WORK_QUEUE;

  protected WorkQueues workQueues;

  protected InProcessPagesDB inProcessPages;

  protected final Object mutex = new Object();
  protected final Object waitingList = new Object();

  protected boolean isFinished = false;
  protected long lastSleepNotification = 0;

  protected long scheduledPages;

  protected DocIDServer docIdServer;
  
  protected Counters counters;
  
  /** An ordered list of the top of the work queue, sorted by priority and docid */
  protected Set<WebURL> current_queue = new TreeSet<WebURL>();
  
  /** A list of seeds that have finished, and so their offspring should be skipped */
  protected Set<Integer> finished_seeds = new HashSet<Integer>();

  public Frontier(Environment env, CrawlConfig config, DocIDServer docIdServer) {
    super(config);
    this.counters = new Counters(env, config);
    this.docIdServer = docIdServer;
    try {
      workQueues = new WorkQueues(env, DATABASE_NAME, config.isResumableCrawling());
      scheduledPages = counters.getValue(Counters.ReservedCounterNames.SCHEDULED_PAGES);
      inProcessPages = new InProcessPagesDB(env, config.isResumableCrawling());
      long numPreviouslyInProcessPages = inProcessPages.getLength();
      if (numPreviouslyInProcessPages > 0) {
        logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
        scheduledPages -= numPreviouslyInProcessPages;
        while (true) {
          List<WebURL> urls = inProcessPages.shift(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          if (urls.size() == 0) {
            break;
          }
          scheduleAll(urls);
        }
      }
    } catch (DatabaseException e) {
      logger.error("Error while initializing the Frontier", e);
      workQueues = null;
    }
  }

  /**
   * Schedule a list of URLs at once, trying to minimize synchronization overhead.
   * 
   * @param urls The list of URLs to schedule
   */
  public void scheduleAll(List<WebURL> urls) {
    int maxPagesToFetch = config.getMaxPagesToFetch();
    Iterator<WebURL> it = urls.iterator();
    synchronized (mutex) {
      while (it.hasNext() && (maxPagesToFetch < 0 || scheduledPages <= maxPagesToFetch)) {
        WebURL url = it.next();
        doSchedule(url);
      }
      counters.setValue(Counters.ReservedCounterNames.SCHEDULED_PAGES, scheduledPages);
    }
    synchronized (waitingList) {
      waitingList.notifyAll();
    }
  }

  /**
   * Private method that actually puts a new URL in the queue. It checks
   * the DocID. If it is -1, it is assumed that this is a newly discovered URL 
   * that should be crawled. If it has already been seen, it is skipped.
   * 
   * @param url The WebURL to schedule
   * @return True if the URL was added to the queue, false otherwise.
   */
  private boolean doSchedule(WebURL url) {
    if (!url.isHttp()) {
      logger.warn("Not scheduling URL {} - Protocol {} not supported", url.getURL(), url.getProtocol());
      return false;
    }
    
    int maxPagesToFetch = config.getMaxPagesToFetch();
    if (maxPagesToFetch >= 0 && scheduledPages >= maxPagesToFetch)
      return false;
      
    if (url.getDocid() < 0) {
      int docid = this.docIdServer.getNewUnseenDocID(url.getURL());
      if (docid == -1)
        return false;
      url.setDocid(docid);
    }
    
    // A URL without a seed doc ID is a seed of itself.
    if (url.getSeedDocid() < 0) {
      url.setSeedDocid(url.getDocid());
    }
      
    try {
      if (workQueues.put(url))
          ++scheduledPages;
    } catch (DatabaseException e) {
      logger.error("Error while putting the url in the work queue", e);
      return false;
    }
    return true;
  }
  
  /**
   * Schedule a WebURL. It will use doSchedule to schedule it and update the counter values.
   * 
   * @param url The WebURL to schedule.
   * @see #doSchedule(WebURL url)
   */
  public boolean schedule(WebURL url) {
    boolean scheduled = false;
    synchronized (mutex) {
      if (scheduled = doSchedule(url))
        counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES);
    }
    
    // Wake up threads
    synchronized (waitingList) {
      waitingList.notifyAll();
    }
    return scheduled;
  }
  
  /**
   * Remove all document IDs from the DocIDServer. This allows to re-crawl
   * pages that have been visited before, which can be useful in a long-running
   * crawler that may revisit pages after a certain amount of time.
   * 
   * This method will wait until all queues are empty to avoid purging DocIDs
   * that are still will be crawled before actually clearing the database, so make
   * sure the crawler is running when executing this method.
   */
  public void clearDocIDs() {
    while (true) {
      if (getQueueLength() > 0) {
        synchronized (waitingList) {
          try {
            waitingList.wait(2000);
          } catch (InterruptedException e)
          {}
        }
      } else {
        synchronized (mutex) {
          if (getQueueLength() > 0)
            continue;
          docIdServer.clear();
          logger.info("Document ID Server has been emptied.");
          break;
        }
      }
    }
  }
  
  /**
   * Add a seed docid that has finished. This is used to determine
   * whether upcoming URLs still need to be crawled. This could be
   * used to abort a seed when it has finished to waste as little time
   * on it as possible.
   * 
   * If the seed doc ID has no offspring in the queue, nothing happens.
   * 
   * @param seed_doc_id The docid of the seed URL to mark as finished.
   */
  public void setSeedFinished(int seed_doc_id) {
    synchronized (mutex) {
      if (numOffspring(seed_doc_id) > 0)
        finished_seeds.add(seed_doc_id);
    }
  }
  
  public WebURL getNextURL(PageFetcher pageFetcher) {
    long last_msg = 0;
    int target_size = config.getFrontierQueueTargetSize();
    int burst = 0;
    
    while (true) {
      long sleep = 0;
      synchronized (mutex) {
        if (isFinished)
          return null;
        
        // Always attempt to keep a decent queue size
        if (current_queue.size() < (0.9 * target_size) || burst > 0) {
          int num_to_get = Math.max(burst,  (int)(1.1 * target_size) - current_queue.size());
          List<WebURL> urls = workQueues.shift(num_to_get);
          for (WebURL url : urls) {
            if (inProcessPages.put(url))
            {
              current_queue.add(url);
            }
          }
          if (burst > 0) {
            if (urls.size() == 0) {
              if (System.currentTimeMillis() - lastSleepNotification < 2000) {
                logger.trace("Politeness delays are long, but no alternative websites are available from the queue");
                lastSleepNotification = System.currentTimeMillis();
              }
              sleep += config.getPolitenessDelay();
            }
          }
          burst = 0;
        }
        
        // Skip URLs at the front of the queue that have already finished
        Iterator<WebURL> iter = current_queue.iterator();
        int num_removed = 0;
        while (iter.hasNext())
        {
            WebURL url = iter.next();
            if (!finished_seeds.contains(url.getSeedDocid()))
                break;
            
            // Seed is finished, so we skip it. It needs to be removed, though.
            if (numOffspring(url.getSeedDocid()) == 1)
            {
                // This is the very last element in the queue. We need to
                // return it to the WebCrawler in order to make sure that
                // handleSeedEnd can be called.
                current_queue.remove(url);
                url.setSeedEnded(true);
                return url;
            }
            
            setProcessed(url);
            iter.remove();
            ++num_removed;
        }
        if (num_removed > 0)
            logger.info("Removed {} elements from the crawl queue because their seed was marked as finished", num_removed);
        
        if (!current_queue.isEmpty())
        {
          WebURL url = pageFetcher.getBestURL(current_queue, config.getPolitenessDelay());
          if (url != null) {
            current_queue.remove(url);
            return url;
          }
          
          long cur = System.currentTimeMillis();
          if (cur > last_msg + 30000)
          {
              logger.debug("Work queue has size {} but timeouts are long. Waiting for more URLs to come in", current_queue.size());
              last_msg = cur;
          }
          
          // No URL can be crawled soon enough, just wait around to see
          // if any better candidate results from current crawling efforts
          sleep += 200;
          burst = (int)(0.25 * target_size);
        }
        else
        {
            long cur = System.currentTimeMillis();
            if (cur > last_msg + 30000)
            {
                logger.debug("Work queue is empty. Waiting for more URLs to come in");
                last_msg = cur;
            }
        }
      }
      
      // Nothing available, wait for more
      synchronized (waitingList) {
        try {
          waitingList.wait(sleep);
        } catch (InterruptedException e)
        {}
        sleep = 0;
      }
    }
  }
  
  /**
   * Set the page as processed and return true if, as a consequence, there is no
   * more offspring left of the seed that eventually resulted in this document.
   * 
   * @param webURL The URL to set as processed
   * @return True when this was the last offspring of the seed, false otherwise
   */
  public boolean setProcessed(WebURL webURL) {
    counters.increment(Counters.ReservedCounterNames.PROCESSED_PAGES);
    synchronized (mutex) {
      if (!inProcessPages.removeURL(webURL)) {
        logger.warn("Could not remove: {} from list of processed pages.", webURL.getURL());
      }
      boolean isLast = numOffspring(webURL.getSeedDocid()) == 0;
      if (isLast)
          finished_seeds.remove(webURL.getSeedDocid());
      return isLast;
    }
  }

  public int numOffspring(Integer seedDocid) {
    synchronized (mutex) {
        return workQueues.getSeedCount(seedDocid) + inProcessPages.getSeedCount(seedDocid);
    }
  }
  
  public long getQueueLength() {
    return getQueueLength(WORK_QUEUE);
  }

  public long getQueueLength(int type) {
    synchronized (mutex) {
      int length = 0;
      if ((type & WORK_QUEUE) == WORK_QUEUE)
          length += workQueues.getLength();
      if ((type & IN_PROGRESS_QUEUE) == IN_PROGRESS_QUEUE)
          length += inProcessPages.getLength();
      return length;
    }
  }

  public long getNumberOfAssignedPages() {
    return getQueueLength(IN_PROGRESS_QUEUE);
  }

  public long getNumberOfProcessedPages() {
    return counters.getValue(Counters.ReservedCounterNames.PROCESSED_PAGES);
  }

  public boolean isFinished() {
    return isFinished;
  }

  public void close() {
    workQueues.close();
    counters.close();
    if (inProcessPages != null) {
      inProcessPages.close();
    }
  }

  public void finish() {
    isFinished = true;
    synchronized (waitingList) {
      waitingList.notifyAll();
    }
  }

  /**
   * Allow a certain piece of code to be run synchronously. This method
   * acquires the mutex and then runs the run method in the provided runnable.
   * 
   * @param r The object on which to run the run method synchronized
   */
  public void runSync(Runnable r) {
      synchronized (mutex) {
          r.run();
      }
  }
}
