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
  protected WorkQueues workQueues;

  protected InProcessPagesDB inProcessPages;

  protected final Object mutex = new Object();
  protected final Object waitingList = new Object();

  protected boolean isFinished = false;

  protected long scheduledPages;

  protected Counters counters;
  
  /** An ordered list of the top of the work queue, sorted by priority and docid */
  protected Set<WebURL> current_queue = new TreeSet<WebURL>();

  public Frontier(Environment env, CrawlConfig config) {
    super(config);
    this.counters = new Counters(env, config);
    try {
      workQueues = new WorkQueues(env, DATABASE_NAME, config.isResumableCrawling());
      if (config.isResumableCrawling()) {
        scheduledPages = counters.getValue(Counters.ReservedCounterNames.SCHEDULED_PAGES);
        inProcessPages = new InProcessPagesDB(env);
        long numPreviouslyInProcessPages = inProcessPages.getLength();
        if (numPreviouslyInProcessPages > 0) {
          logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
          scheduledPages -= numPreviouslyInProcessPages;

          List<WebURL> urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          while (!urls.isEmpty()) {
            scheduleAll(urls);
            inProcessPages.delete(urls.size());
            urls = inProcessPages.get(IN_PROCESS_RESCHEDULE_BATCH_SIZE);
          }
        }
      } else {
        inProcessPages = null;
        scheduledPages = 0;
      }
    } catch (DatabaseException e) {
      logger.error("Error while initializing the Frontier", e);
      workQueues = null;
    }
  }

  public void scheduleAll(List<WebURL> urls) {
    int maxPagesToFetch = config.getMaxPagesToFetch();
    synchronized (mutex) {
      int newScheduledPage = 0;
      for (WebURL url : urls) {
        if ((maxPagesToFetch > 0) && ((scheduledPages + newScheduledPage) >= maxPagesToFetch)) {
          break;
        }

        try {
          workQueues.put(url);
          newScheduledPage++;
        } catch (DatabaseException e) {
          logger.error("Error while putting the url in the work queue", e);
        }
      }
      if (newScheduledPage > 0) {
        scheduledPages += newScheduledPage;
        counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES, newScheduledPage);
      }
      synchronized (waitingList) {
        waitingList.notifyAll();
      }
    }
  }

  public void schedule(WebURL url) {
    int maxPagesToFetch = config.getMaxPagesToFetch();
    synchronized (mutex) {
      try {
        if (maxPagesToFetch < 0 || scheduledPages < maxPagesToFetch) {
          workQueues.put(url);
          scheduledPages++;
          counters.increment(Counters.ReservedCounterNames.SCHEDULED_PAGES);
        }
      } catch (DatabaseException e) {
        logger.error("Error while putting the url in the work queue", e);
      }
    }
  }
  
  public WebURL getNextURL(PageFetcher pageFetcher) {
    while (true)
    {
      synchronized (mutex) {
        if (isFinished)
          return null;
        
        // Always attempt to keep a decent queue size
        if (current_queue.size() < 25) {
          List<WebURL> urls = workQueues.shift(100);
          for (WebURL url : urls) {
            if (inProcessPages.put(url))
              current_queue.add(url);
          }
        }
        
        if (!current_queue.isEmpty())
        {
          WebURL url = pageFetcher.getBestURL(current_queue);
          current_queue.remove(url);
          return url;
        }
      }
      
      // Nothing available, wait for more
      synchronized (waitingList) {
        try {
          waitingList.wait();
        } catch (InterruptedException e)
        {}
      }
    }
  }

  public void setProcessed(WebURL webURL) {
    counters.increment(Counters.ReservedCounterNames.PROCESSED_PAGES);
    if (inProcessPages != null) {
      if (!inProcessPages.removeURL(webURL)) {
        logger.warn("Could not remove: {} from list of processed pages.", webURL.getURL());
      }
    }
  }

  public long getQueueLength() {
    return workQueues.getLength();
  }

  public long getNumberOfAssignedPages() {
    return inProcessPages.getLength();
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
}
