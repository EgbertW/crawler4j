package edu.uci.ics.crawler4j.frontier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;

public class BerkeleyDBQueue extends AbstractCrawlQueue {
  public static final Logger logger = LoggerFactory.getLogger(BerkeleyDBQueue.class);
  private WorkQueues workqueue;
  private InProcessPagesDB shortqueue;
  private ArrayList<WebURL> queue = new ArrayList<WebURL>();
  private ArrayList<WebURL> inProgress = new ArrayList<WebURL>();
  private HashMap<Long, Integer> seed_counter = new HashMap<Long, Integer>();
  
  public BerkeleyDBQueue(Environment env)
  {
    workqueue = new WorkQueues(env, "CrawlQueue", config.isResumableCrawling());
    shortqueue = new InProcessPagesDB(env, config.isResumableCrawling());
    
    try {
      long numPreviouslyInProcessPages = shortqueue.getLength();
      if (numPreviouslyInProcessPages > 0) {
        logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
        while (true) {
          List<WebURL> urls = shortqueue.shift(100);
          if (urls.size() == 0) {
            break;
          }
          enqueue(urls);
        }
      }
    } catch (DatabaseException e) {
      logger.error("Error while initializing the crawl queue", e);
      throw e;
    }
  }
  
  @Override
  public void enqueue(WebURL url) {
    if (!workqueue.put(url))
      throw new RuntimeException("Failed to add element to the list");
  }

  @Override
  public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) {
    if (queue.size() < config.getFrontierQueueTargetSize() * 0.75) {
      int max = (int)Math.ceil(config.getFrontierQueueTargetSize() * 1.25) - queue.size();
      List<WebURL> urls = workqueue.shift(max);
      for (WebURL url : urls)
      {
        Integer curcnt = seed_counter.get(url.getSeedDocid());
        if (curcnt == null)
          curcnt = 0;
        
        seed_counter.put(url.getSeedDocid(), curcnt + 1);
        queue.add(url);     
        shortqueue.put(url);
      }
    }
    
    WebURL best = fetcher.getBestURL(queue, config.getPolitenessDelay());
    if (best != null) {
      queue.remove(best);
      inProgress.add(best);
    }
    
    return best;
  }

  @Override
  public void abandon(WebCrawler crawler, WebURL url) {
    if (!inProgress.contains(url))
      throw new RuntimeException("Cannot abandon URL - not in progress: " + url.getURL());
    
    inProgress.remove(url);
    queue.add(url);
  }

  @Override
  public void setFinishedURL(WebCrawler crawler, WebURL url) {
    if (!inProgress.contains(url))
      throw new RuntimeException("Cannot finish URL - not in progress: " + url.getURL());

    inProgress.remove(url);
    shortqueue.removeURL(url);
    Integer cnt = seed_counter.get(url.getSeedDocid());
    if (cnt == 1)
      seed_counter.remove(url.getSeedDocid());
    else
      seed_counter.put(url.getSeedDocid(), cnt - 1);
  }

  @Override
  public long getQueueSize() {
    return inProgress.size() + queue.size() + workqueue.getLength();
  }

  @Override
  public long getNumInProgress() {
    return inProgress.size();
  }

  @Override
  public long getNumOffspring(long seed_doc_id) {
    int count = workqueue.getSeedCount(seed_doc_id);
    
    Integer local = seed_counter.get(seed_doc_id);
    if (local != null)
      count += local;
        
    return count;
  }

  @Override
  public void setSeedFinished(long seed_doc_id) {
    workqueue.removeOffspring(seed_doc_id);
    shortqueue.removeOffspring(seed_doc_id);
    
    // Remove all locally queued elements that are offspring
    Iterator<WebURL> iter = queue.iterator();
    while (iter.hasNext())
    {
      WebURL url = iter.next();
      if (url.getSeedDocid() == seed_doc_id)
        iter.remove();
    }
  }
}