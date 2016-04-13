package edu.uci.ics.crawler4j.frontier;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IterateAction;
import edu.uci.ics.crawler4j.util.Processor;
import edu.uci.ics.crawler4j.util.Util.Reference;

/**
 * This is an implementation of a URL queue using the BerkeleyDB backed
 * queue in the URLQueue class. It provides resumable crawling by
 * storing all URLs in the database on disk and good performance.
 * 
 * By using is shorter queue that is stored in memory, it is possible to
 * select a better URL based on the politeness delay involved in the urls.
 * 
 * @author Egbert van der Wal
 */
public class BerkeleyDBQueue extends AbstractCrawlQueue {
  public static final Logger logger = LoggerFactory.getLogger(BerkeleyDBQueue.class);
  
  /** The long crawl queue. This is a pure BerkeleyDB backed queue, which may contain
   * very many elements. */
  private URLQueue crawl_queue_db;
  
  /** The short crawl queue. This is the BerkeleyDB backed queue to make it persistent,
   * but most operations are executed on the urls_unassigned queue and the urls_in_progress
   * list.
   */
  private URLQueue in_progress_db;
  
  /**
   * Initialize the queue and reload stored elements from disk.
   * 
   * @param env The BerkeleyDB environment
   * @param config The crawl configuration, mainly for the politeness delay parameter
   */
  public BerkeleyDBQueue(Environment env, CrawlConfig config)
  {
    setCrawlConfiguration(config);;
    logger.error("Setting up BerkeleyDBQueue as crawl queue");
    crawl_queue_db = new URLQueue(env, "CrawlQueue", config.isResumableCrawling());
    in_progress_db = new URLQueue(env, "InProgress", config.isResumableCrawling());
    
    try {
      long numPreviouslyInProcessPages = in_progress_db.getLength();
      if (numPreviouslyInProcessPages > 0) {
        logger.info("Rescheduling {} URLs from previous crawl.", numPreviouslyInProcessPages);
        List<WebURL> urls = in_progress_db.getDump();
        enqueue(urls);
      }
    } catch (DatabaseException e) {
      logger.error("Error while initializing the crawl queue", e);
      throw e;
    }
  }
  
  @Override
  public boolean enqueue(WebURL url) {
    if (!crawl_queue_db.put(url)) {
      last_error = "Duplicate URL";
      return false;
    }
    return true;
  }
  
  @Override
  public List<WebURL> enqueue(List<WebURL> urls) {
    return crawl_queue_db.put(urls);
  }

  @Override
  public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) {
    WebURL oldURL = getAssignedURL(crawler);
    if (oldURL != null)
      throw new RuntimeException("Crawler " + crawler.getMyId() + " has not finished its previous URL: " + oldURL.getURL() + " (" + oldURL.getDocid() + ")");
    
    final Reference<Long> best_time = new Reference<Long>(Long.MAX_VALUE);
    final Reference<WebURL> best_url = new Reference<WebURL>(null);
    final Reference<Integer> counter = new Reference<Integer>(0);
    final long max_end_time = System.currentTimeMillis() + Math.max(100, config.getPolitenessDelay());
    final Map<String, Long> delays = fetcher.getHostMap();
    
    long st = System.currentTimeMillis();
    crawl_queue_db.iterate(new Processor<WebURL, IterateAction>() {
      long now = System.currentTimeMillis();
      
      public IterateAction apply(WebURL url) {
        ++counter.val;
        String host = url.getURI().getHost();
        if (!delays.containsKey(host)) {
          best_time.val = 0l;
          best_url.val = url;
          return IterateAction.RETURN;
        }
        
        Long t = delays.get(host);
        if (t < best_time.get()) {
          best_time.val = t;
          best_url.val = url;
        }
        
        // Only update time every 1000 iterations to save syscalls
        if (counter.val % 1000 == 0) {
          now = System.currentTimeMillis();
          
          // Don't try for to find something for longer than the politeness delay
          // as that kinda defeats the purpose. If any match is now int the past,
          // we're also done.
          if (max_end_time <= now || best_time.val < now)
            return IterateAction.RETURN;
          
          if (best_time.val < now)
            return IterateAction.RETURN;
        }
        
        return IterateAction.CONTINUE;
      }
    });

    long dur = System.currentTimeMillis() - st;
    if (dur > config.getPolitenessDelay() * 0.5)
      logger.info("Considered {} URLs in {}ms (limit: {})", counter.val, dur, config.getPolitenessDelay());
    
    Long delay = best_time.val - System.currentTimeMillis();
    WebURL best = best_url.val;
    if (best == null)
      return null;
    
    if (best != null && delay < config.getPolitenessDelay()) {
      crawl_queue_db.removeURL(best);
      assign(best, crawler);
      in_progress_db.put(best);
      fetcher.select(best);
      return best;
    }
    
    return null;
  }

  @Override
  public void abandon(WebCrawler crawler, WebURL url) {
    unassign(url, crawler);
    in_progress_db.removeURL(url);
    crawl_queue_db.put(url);
  }

  @Override
  public void setFinishedURL(WebCrawler crawler, WebURL url) {
    unassign(url, crawler);
    in_progress_db.removeURL(url);
  }

  @Override
  public long getQueueSize() {
    return crawl_queue_db.getLength() + in_progress_db.getLength();
  }

  @Override
  public long getNumOffspring(long seed_doc_id) {
    return crawl_queue_db.getSeedCount(seed_doc_id) + in_progress_db.getSeedCount(seed_doc_id);
  }

  @Override
  public void removeOffspring(long seed_doc_id) {
    crawl_queue_db.removeOffspring(seed_doc_id);
  }
}