package edu.uci.ics.crawler4j.frontier;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

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
import edu.uci.ics.crawler4j.util.Util;

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
  
  private PageFetcher fetcher;
  
  private static class HostQueue {
    String host;
    long nextFetchTime;
    
    WebURL head;
    WebURL tail;
    
    HostQueue(String host) {
      this.host = host;
    }
  }
  
  private class HostComparator implements Comparator<HostQueue> {
    HostComparator(Map<String, Long> nextFetchTimes) {
      for (Map.Entry<String, HostQueue> e : host_queue.entrySet()) {
        Long nft = nextFetchTimes.get(e.getKey());
        e.getValue().nextFetchTime = nft == null ? 0 : nft;
      }
    }
    
    @Override
    public int compare(HostQueue lhs, HostQueue rhs) {
      if (lhs.nextFetchTime != rhs.nextFetchTime)
        return Long.compare(lhs.nextFetchTime,  rhs.nextFetchTime);
      
      if (lhs.head.getPriority() != rhs.head.getPriority())
        return lhs.head.getPriority() - rhs.head.getPriority();
      
      return lhs.host.compareTo(rhs.host);
    }
  }
  
  private HashMap<String, HostQueue> host_queue = new HashMap<String, HostQueue>();
  
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
      // All URLs should be in order of their keys, so we can just always
      // do a tail insert
      crawl_queue_db.iterate(new Processor<WebURL, IterateAction>() {
        @Override
        public IterateAction apply(WebURL url) {
          String host = url.getURI().getHost();
          HostQueue hq = host_queue.get(host);
          
          if (hq == null) {
            hq = new HostQueue(host);
            hq.head = hq.tail = url;
            host_queue.put(host, hq);
          } else {
            hq.tail = url;
          }
          return IterateAction.CONTINUE;
        }
      });
      
      long numPreviouslyInProcessPages = in_progress_db.getLength();
      if (numPreviouslyInProcessPages > 0) {
        // All urls that were in progress need to be added to the queue, so
        // that they may be reassigned to a crawler.
        List<WebURL> urls = in_progress_db.shift((int)numPreviouslyInProcessPages);
        enqueue(urls);
      }
      
      if (crawl_queue_db.getLength() > 0)
        logger.info("Reconstructed crawl queue with {} hosts and {} URLs", host_queue.size(), crawl_queue_db.getLength());
    } catch (DatabaseException e) {
      logger.error("Error while initializing the crawl queue", e);
      throw e;
    }
  }
  
  @Override
  public boolean enqueue(WebURL url) {
    URI uri = url.getURI();
    String host = uri.getHost();
    
    HostQueue hq = host_queue.get(host);
    boolean result = true;
    if (hq == null) {
      url.setPrevious((byte []) null);
      url.setNext((byte []) null);
      hq = new HostQueue(host);
      hq.head = url;
      hq.tail = url;
      
      result &= host_queue.put(host, hq) == null;
      result &= crawl_queue_db.put(url);
      return result;
    }
    
    // Need to insert it in the queue at the appropriate position
    if (url.compareTo(hq.tail) >= 0) {
      // Tail-insert
      WebURL tail = hq.tail;
      url.setPrevious(tail);
      tail.setNext(url);
      
      hq.tail = url;
      result &= crawl_queue_db.update(tail);
      result &= crawl_queue_db.put(url);
      return result;
    }
    
    if (url.compareTo(hq.head) < 0) {
      // Head-insert
      WebURL head = hq.head;
      url.setNext(head);
      head.setPrevious(url);
      
      hq.head = url;
      result &= crawl_queue_db.update(head);
      result &= crawl_queue_db.put(url);
      return result;
    }
    
    // It's not the last element, so walk the queue to find the proper location
    WebURL cur = hq.head;
    WebURL prev = null;
    while (cur != null && url.compareTo(cur) >= 0) {
      prev = cur;
      byte [] next_key = cur.getNext();
      cur = crawl_queue_db.get(next_key);
      if (next_key != null && cur == null) {
        logger.error("--- ERRROR!!!! FOR HOST {}, URL {} IS SUPPOSED TO HAVE NEXT BUT NEXT DOESNT EXIST", host, prev.getURL());
        throw new RuntimeException("Next_key does not exist in crawl db");
      }
    }
    
    // The previous position needs to be non-null now, because otherwise it should've
    // been handled by the head-insert above
    if (prev == null) {
      logger.error("THIS IS WRONG -> prev == null, which implies a head insert - walking the queue ");
      logger.error("URL to insert: {} Docid: {}, priority: {}", url.getURL(), url.getDocid(), url.getPriority());
      cur = hq.head;
      int i = 0;
      while (cur != null) {
        String special = "";
        if (cur.getDocid() == hq.head.getDocid())
          special += "[HEAD]";
        if (cur.getDocid() == hq.tail.getDocid())
          special += "[TAIL]";
        logger.error("Pos: {} {} Host: {} URL: {} - Docid: {} Prio: {}", i++, special, host, cur.getURL(), cur.getDocid(), cur.getPriority());
        byte [] next_key = cur.getNext();
        cur = crawl_queue_db.get(next_key);
        if (cur == null && next_key != null)
          logger.error("------------- URL COULD NOT BE FOUND OF getNext(); WHIILE IT IS NOT NULL!");
      }
      logger.error("---- END OF QUEUE FOR HOST: {}", host);
      throw new RuntimeException("Prev is null, but no head-insert has been performed");
    }
    
    // The current position needs to be non-null now, because otherwise it should've
    // been handled by the tail-insert above
    if (cur == null) {
      logger.error("THIS IS WRONG -> cur == null, which implies a tail insert - walking the queue ");
      logger.error("URL to insert: {} Docid: {}, priority: {}", url.getURL(), url.getDocid(), url.getPriority());
      cur = hq.head;
      int i = 0;
      while (cur != null) {
        String special = "";
        if (cur.getDocid() == hq.head.getDocid())
          special += "[HEAD]";
        if (cur.getDocid() == hq.tail.getDocid())
          special += "[TAIL]";
        logger.error("Pos: {} {} Host: {} URL: {} - Docid: {} Prio: {}", i++, special, host, cur.getURL(), cur.getDocid(), cur.getPriority());
        byte [] next_key = cur.getNext();
        cur = crawl_queue_db.get(next_key);
        if (cur == null && next_key != null)
          logger.error("------------- URL COULD NOT BE FOUND OF getNext(); WHIILE IT IS NOT NULL!");
      }
      logger.error("---- END OF QUEUE FOR HOST: {}", host);
      throw new RuntimeException("Cur is null, but no tail-insert has been performed");
    }
    
    // Need to insert between prev and cur
    prev.setNext(url);
    url.setPrevious(prev);
    url.setNext(cur);
    cur.setPrevious(url);
    
    result &= crawl_queue_db.update(prev);
    result &= crawl_queue_db.update(cur);
    result &= crawl_queue_db.put(url);
    
    return result;
  }
  
  @Override
  public List<WebURL> enqueue(List<WebURL> urls) {
    // Sort the URLs on their priority first, to make
    // inserting them in the queue the faster. If we assume
    // that all URLs will end up in at the end of the list,
    // we will do only tail inserts which are handled separately.
    TreeSet<WebURL> sorted = new TreeSet<WebURL>(urls);
    
    ArrayList<WebURL> rejects = new ArrayList<WebURL>();
    for (WebURL url : sorted)
      if (!enqueue(url)) {
        rejects.add(url);
      }
    
    return rejects;
  }

  @Override
  public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) {
    this.fetcher = fetcher;
    WebURL oldURL = getAssignedURL(crawler);
    if (oldURL != null)
      throw new RuntimeException("Crawler " + crawler.getMyId() + " has not finished its previous URL: " + oldURL.getURL() + " (" + oldURL.getDocid() + ")");
    
    if (host_queue.isEmpty())
      return null;
      
    long start = System.nanoTime();
    // Sort the available hosts based on priority and delay
    TreeSet<HostQueue> sorted = new TreeSet<HostQueue>(new HostComparator(fetcher.getHostMap()));
    sorted.addAll(host_queue.values());
    
    // The head of the list is the best candidate
    HostQueue best = sorted.first();
    long threshold = System.currentTimeMillis() + config.getPolitenessDelay();
    long dur = System.nanoTime() - start;
    
    double durr = Math.round(dur / 1000.0);
    logger.info("Sorting and selecting best URL out of {} hosts took {} microseconds", sorted.size(), durr);
    if (best.nextFetchTime > threshold)
      return null;
    
    WebURL best_url = best.head;
    if (best_url == null)
      throw new RuntimeException("best.head is null. This should not happen -> check enqueue / disqueue administration");
    
    // We already know most relevant facts, so lets do the update directly
    byte[] next_key = best_url.getNext();
    if (next_key == null) {
      // The queue is empty
      host_queue.remove(best.host);
    } else {
      // Move the head to the next element
      best.head = crawl_queue_db.get(next_key);
    }
    
    crawl_queue_db.removeURL(best_url);
    assign(best_url, crawler);
    in_progress_db.put(best_url);
    fetcher.select(best_url);
    return best_url;
  }

  @Override
  public void abandon(WebCrawler crawler, WebURL url) {
    logger.info("Crawler {} abandons URL {}", crawler.getMyId(), url.getURL());
    unassign(url, crawler);
    in_progress_db.removeURL(url);
    fetcher.unselect(url);
    enqueue(url);
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
    final Util.Reference<Integer> num_removed = new Util.Reference<Integer>(0);
    crawl_queue_db.iterate(new Processor<WebURL, IterateAction>() {
      @Override
      public IterateAction apply(WebURL url) {
        if (url.getSeedDocid() == seed_doc_id) {
          num_removed.assign(num_removed.get() + 1);
          WebURL prev = crawl_queue_db.get(url.getPrevious());
          WebURL next = crawl_queue_db.get(url.getNext());
    
          if (prev != null) {
            prev.setNext(next);
            crawl_queue_db.update(prev);
          }
          
          if (next != null) {
            next.setPrevious(prev);
            crawl_queue_db.update(next);
          }
          
          String host = url.getURI().getHost();
          HostQueue hq = host_queue.get(host);
          assert(hq != null);
          if (hq.head.getDocid() == url.getDocid()) {
            if (next == null)
              host_queue.remove(host);
            else
              hq.head = next;
          }
          
          if (hq.tail.getDocid() == url.getDocid()) {
            if (prev == null)
              host_queue.remove(host);
            else
              hq.tail = prev;
          }
             
          return IterateAction.REMOVE;
        }
        return IterateAction.CONTINUE;
      }
    });
  }
}