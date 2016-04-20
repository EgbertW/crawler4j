package edu.uci.ics.crawler4j.frontier;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  
  // TODO: this should be moved to tests
  @SuppressWarnings("unused")
  private void validateQueue() {
    // PERFORMANCE KILLER BUT NECESSARY FOR DEBUG
    long st = System.nanoTime();
    for (Map.Entry<String, HostQueue> e : host_queue.entrySet()) {
      String host = e.getKey();
      HostQueue hq = e.getValue();
      
      if (hq.head == null)
        die(hq, new Throwable("No head in host queue: " + host));
      
      if (hq.tail == null)
        die(hq, new Throwable("No tail in host queue: " + host));
      
      WebURL cur = crawl_queue_db.get(hq.head.getKey());
      if (cur == null)
        die(hq, new Throwable("Head not found in database: host: " + host + " head: " + hq.head.getURL() + " docid: ##" + hq.head.getDocid() + "##"));
      if (cur.getDocid() != hq.head.getDocid())
        die(hq, new Throwable("Head docid does not match - db says: ##" + cur.getDocid() + "## hq says: ##" + hq.head.getDocid() + "##"));
      
      WebURL tail_check = crawl_queue_db.get(hq.tail.getKey());
      if (tail_check == null)
        die(hq, new Throwable("Tail not found in database: host: " + host + " head: " + hq.tail.getURL() + " docid: ##" + hq.tail.getDocid() + "##"));
      
      if (tail_check.getDocid() != hq.tail.getDocid())
        die(hq, new Throwable("Tail docid does not match - db says: ##" + cur.getDocid() + "## hq says: ##" + hq.head.getDocid() + "##"));
      
      WebURL prev = null;
      WebURL next = null;
      int i = 0;
      while (cur != null) {
        byte [] next_key = cur.getNext();
        if (next_key != null) {
          next = crawl_queue_db.get(next_key);
          if (next == null)
            die(hq, new Throwable("Host: " + host + " @ pos: " + (i) + " - Next_key is not null, but URL does not exist in database"));
          
          if (cur.getDocid() == hq.tail.getDocid()) {
            die(hq, new Throwable("Host: " + host + " @ pos: " + i + " - Current element is tail, but there is a next_url: " + next.getURL() + " (Docid: ##" + next.getDocid() + "##"));
          }
        } else {
          next = null;
        }
        
        byte [] prev_key = cur.getPrevious();
        if (prev_key != null) {
          WebURL tmp_prev = crawl_queue_db.get(prev_key);
          if (tmp_prev == null)
            die(hq, new Throwable("Host: " + host + " @ pos: " + i + " - prev_key exists but cannot be found in db"));
            
          if (prev == null)
            die(hq, new Throwable("Host: " + host + " @ pos: " + (i) + " - prev_key is not null, but URL is head of the queue - URL: " + tmp_prev.getURL() + " (Docid: ##" + tmp_prev.getDocid() + "##"));
          
          if (tmp_prev.getDocid() != prev.getDocid())
            die(hq, new Throwable("Host: " + host + " @ pos: " + (i) + " - prev.getDocid(##" + prev.getDocid() + "##) != tmp_prev.getDocid(##" + tmp_prev.getDocid() + "##)"));
        } else if (prev != null) {
          die(hq, new Throwable("Host: " + host + " @ pos: " + (i) + " - prev_key is null, but prev != null"));
        }
        
        prev = cur;
        cur = next;
        ++i;
      }
      if (prev == null)
        die(hq, new Throwable("Host: " + host + " @ pos: "+ i + " - prev is null at end of queue"));
      
      if (prev.getDocid() != hq.tail.getDocid())
        die(hq, new Throwable("Prev.getDocid(##" + prev.getDocid() + "##) != hq.tail.getDocid(##" + hq.tail.getDocid() + "##)"));
    }
    long dur = System.nanoTime() - st;
    logger.info("Validating host_queue took {} ms", Math.round(dur / 10000.0) / 100.0);
  }
  
  private void die(HostQueue hq, Throwable e) {
    logger.error("ERROR: {}", e.getMessage());
    System.err.println("ERROR: " + e.getMessage());
    if (e != null) {
      logger.error("Stacktrace: ", e);
      e.printStackTrace(System.err);
    }
    
    logger.error("-- LISTING QUEUE");
    WebURL cur = hq.head;
    String host = hq.host;
    int i = 0;
    while (cur != null) {
      String special = "";
      if (cur.getDocid() == hq.head.getDocid())
        special += "[HEAD]";
      if (cur.getDocid() == hq.tail.getDocid())
        special += "[TAIL]";
      logger.error("Pos: {} {} Host: {} URL: {} - Docid: ##{}## Prio: {}", i++, special, host, cur.getURL(), cur.getDocid(), cur.getPriority());
      byte [] next_key = cur.getNext();
      cur = crawl_queue_db.get(next_key);
      if (cur == null && next_key != null)
        logger.error("------------- URL COULD NOT BE FOUND OF getNext(); WHIILE IT IS NOT NULL!");
    }
    logger.error("Pos: TAIL Host: {} URL: {} - Docid: ##{}## Prio: {}", host, hq.tail.getURL(), hq.tail.getDocid(), hq.tail.getPriority());
    logger.error("---- END OF QUEUE FOR HOST: {}", host);
    
    System.exit(1);
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
    
    int comp;
    
    // Need to insert it in the queue at the appropriate position
    if ((comp = url.compareTo(hq.tail)) >= 0) {
      if (comp == 0)
        throw new RuntimeException("Duplicate URL");
      
      // Tail-insert
      WebURL tail = hq.tail;
      url.setPrevious(tail);
      url.setNext((byte []) null);
      tail.setNext(url);
      
      hq.tail = url;
      result &= crawl_queue_db.update(tail);
      result &= crawl_queue_db.put(url);
      return result;
    }
    
    if (url.compareTo(hq.head) < 0) {
      // Head-insert
      WebURL head = hq.head;
      url.setPrevious((byte []) null);
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
    while (cur != null && (comp = url.compareTo(cur)) >= 0) {
      if (comp == 0) {
        last_error = "Duplicate URL";
        return false;
      }
      
      prev = cur;
      byte [] next_key = cur.getNext();
      cur = crawl_queue_db.get(next_key);
    }
    
    // The previous position needs to be non-null now, because otherwise it should've
    // been handled by the head-insert above
    if (prev == null)
      throw new RuntimeException("Prev is null, but no head-insert has been performed");
    
    // The current position needs to be non-null now, because otherwise it should've
    // been handled by the tail-insert above
    if (cur == null)
      throw new RuntimeException("Cur is null, but no tail-insert has been performed");
    
    // When doing a mid-insert after head or before tail, we need to make
    // sure that hq.head and hq.tail are updated, otherwise inconsistencies 
    // will arise.
    if (prev.getDocid() == hq.head.getDocid())
      prev = hq.head;
    if (cur.getDocid() == hq.tail.getDocid())
      cur = hq.tail;
    
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
  public List<WebURL> enqueue(Collection<WebURL> urls) {
    ArrayList<WebURL> rejects = new ArrayList<WebURL>();
    
    HashMap<String, Set<WebURL>> new_urls = new HashMap<String, Set<WebURL>>();
    
    // Sort the URLs into their hosts, and for each host, sort on priority
    for (WebURL url : urls){
      String host = url.getURI().getHost();
      if (!new_urls.containsKey(host))
        new_urls.put(host, new TreeSet<WebURL>());
      new_urls.get(host).add(url);
    }
    
    for (Map.Entry<String, Set<WebURL>> e : new_urls.entrySet()) {
      String host = e.getKey();
      Set<WebURL> host_urls = e.getValue();
      HostQueue hq = host_queue.get(host);
      
      if (hq == null) {
        hq = new HostQueue(host);
        hq.head = hq.tail = null;
        host_queue.put(host, hq);
      }
      
      WebURL cursor = hq.head;
      WebURL prev = null;
      
      Set<WebURL> urls_to_insert = new HashSet<WebURL>();
      Set<WebURL> urls_to_update = new HashSet<WebURL>();
      
      Iterator<WebURL> new_iter = host_urls.iterator();
      int comparison;
      while (new_iter.hasNext()) {
        WebURL to_insert = new_iter.next();
        
        comparison = -1;
        // Advance to the point where to where the new URL should be inserted before it
        while (cursor != null && (comparison = to_insert.compareTo(cursor)) >= 0) {
          if (comparison == 0)
            break;
          
          prev = cursor;
          byte [] next_key = cursor.getNext();
          if (hq.tail != null && next_key != null && hq.tail.compareKey(next_key) == 0)
            cursor = hq.tail;
          else
            cursor = crawl_queue_db.get(next_key);
        }
        
        if (comparison == 0) { 
          rejects.add(to_insert);
          continue;
        }
        
        // At this point, either cursor == null, meaning we do a tail insert, or
        // to_insert.compareTo(cursor) < 0, meaning the new URL should be inserted
        // before the current URL
        if (hq.head == null) {
          to_insert.setPrevious((byte []) null);
          to_insert.setNext((byte []) null);
          hq.head = hq.tail = prev = to_insert;
        } else if (cursor == null) { // Tail insert -> prev == hq.tail
          hq.tail.setNext(to_insert);
          to_insert.setPrevious(hq.tail);
          to_insert.setNext((byte []) null);
          if (!urls_to_insert.contains(hq.tail))
            urls_to_update.add(hq.tail);
          
          hq.tail = to_insert;
        } else if (prev == null) { // Head insert, cursor == hq.head
          to_insert.setPrevious((byte []) null);
          to_insert.setNext(hq.head);
          
          hq.head.setPrevious(to_insert);
          if (!urls_to_insert.contains(hq.head))
            urls_to_update.add(hq.head);
          
          hq.head = to_insert;
        } else { // In between insert prev and cur
          prev.setNext(to_insert);
          to_insert.setPrevious(prev);
          to_insert.setNext(cursor);
          cursor.setPrevious(to_insert);
          
          if (!urls_to_insert.contains(prev))
            urls_to_update.add(prev);
          if (!urls_to_insert.contains(cursor))
            urls_to_update.add(cursor);
        }
        urls_to_insert.add(to_insert);
        new_iter.remove();
      }
      
      // Insert all new urls in the database
      crawl_queue_db.put(urls_to_insert);
      
      // Update all previously known urls
      crawl_queue_db.update(urls_to_update);
    }
    
    // Make sure we did well ;-)
    //validateQueue();
    
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
    long delay = Math.max(0, best.nextFetchTime - System.currentTimeMillis());
    long dur = System.nanoTime() - start;
    
    double durr = Math.round(dur / 1000.0) / 1000.0;
    logger.info("Sorting and selecting best URL out of {} hosts took {}ms -> resulting delay: {}ms for host {}", sorted.size(), durr, delay, best.host);
    if (best.nextFetchTime > threshold)
      return null;
    
    WebURL best_url = best.head;
    if (best_url == null)
      throw new RuntimeException("best.head is null. This should not happen -> check enqueue / disqueue administration");
    
    // Update the linkedlist in the crawl queue
    byte[] next_key = best_url.getNext();
    if (next_key == null) {
      // The queue is empty
      host_queue.remove(best.host);
    } else {
      // Move the head to the next element
      best.head = crawl_queue_db.get(next_key);
      best.head.setPrevious((byte []) null);
      crawl_queue_db.update(best.head);
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
          ++num_removed.val;
          
          String host = url.getURI().getHost();
          HostQueue hq = host_queue.get(host);
          
          if (hq == null)
            throw new RuntimeException("Element in URL queue is not in host_queue - docid: " + url.getDocid() + " host: " + host);
          
          WebURL prev = crawl_queue_db.get(url.getPrevious());
          WebURL next = crawl_queue_db.get(url.getNext());
    
          if (prev != null && prev.getDocid() == hq.head.getDocid())
            prev = hq.head;
          
          if (next != null && next.getDocid() == hq.tail.getDocid())
            next = hq.tail;
          
          if (prev != null) {
            prev.setNext(next);
            crawl_queue_db.update(prev);
          }
          
          if (next != null) {
            next.setPrevious(prev);
            crawl_queue_db.update(next);
          }
          
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
    logger.info("Removed {} offspring of seed [[{}]]", num_removed.val, seed_doc_id);
  }
}