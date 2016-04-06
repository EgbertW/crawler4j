package edu.uci.ics.crawler4j.frontier;

import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IterateAction;
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
   */
  public BerkeleyDBQueue(Environment env)
  {
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
  public void enqueue(WebURL url) {
    if (!crawl_queue_db.put(url))
      throw new RuntimeException("Failed to add element to the list");
  }
  
  @Override
  public List<WebURL> enqueue(List<WebURL> urls) {
    return crawl_queue_db.put(urls);
  }

  @Override
  public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) {
    if (urls_in_progress.get(crawler.getId()) != null)
      throw new RuntimeException("Crawler " + crawler.getId() + " has not finished its previous URL");
    
    final Reference<Long> shortest_delay = new Reference<Long>(Long.MAX_VALUE);
    final Reference<WebURL> best_url = new Reference<WebURL>(null);
    
    crawl_queue_db.iterate(new Function<WebURL, IterateAction>() {
      public IterateAction apply(WebURL url) {
        long delay = fetcher.getFetchDelay(url);
        if (delay < shortest_delay.get()) {
          best_url.assign(url);
          shortest_delay.assign(delay);
        }

        // We won't find any better URL than this, so stop.
        if (delay == 0)
          return IterateAction.REMOVE_AND_RETURN;
          
        return IterateAction.CONTINUE;
      }
    });

    Long delay = shortest_delay.get();
    WebURL best = best_url.get();
    if (delay < config.getPolitenessDelay()) {
      crawl_queue_db.removeURL(best);
      assign(best, crawler);
      in_progress_db.put(best);
      
      return best;
    }
    
    return best;
  }

  @Override
  public void abandon(WebCrawler crawler, WebURL url) {
    unassign(url, crawler);
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