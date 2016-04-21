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
 * queue in the URLQueue class. It maintains a crawling queue sorted on 
 * host names on which the URLs reside. A list of hosts with queued URLs
 * is therefore always available, making it easy to select a host that
 * has the shortest possible crawl delay, and once a host has been selected,
 * the best URL from that host to fetch.
 * 
 * This is achieved in combination with the PageFetcher that maintains the crawl
 * delay data based on the politeness delay setting.
 * 
 * @author Egbert van der Wal
 */
public class BerkeleyDBQueue extends AbstractCrawlQueue {
  public static final Logger logger = LoggerFactory.getLogger(BerkeleyDBQueue.class);
  
  /** 
   * The crawl queue. This is a pure BerkeleyDB backed queue, which contains
   * all the URLs that need to be retrieved and have not been assigned to a
   * crawler yet.
   */
  protected URLQueue crawl_queue_db;
  
  /** 
   * The list of URLs that are in progress -&gt; have been assigned to a crawler.
   * When a URL is assigned using getNextURL, it will be moved from the 
   * crawl_queue_db to the in_progress_db queue from which it will be removed
   * as soon as the page has been retrieved or cancelled using setFinished or
   * abandon.
   */
  protected URLQueue in_progress_db;
  
  protected PageFetcher fetcher;
  
  /**
   * Maintain the list of host-specific crawl queues. Each crawl
   * queue is a linked list where the head and the tail are stored,
   * so that head and tail inserts can be done efficiently. This class
   * is used to quickly determine the best host and the best URL on that
   * host to fetch next.
   * 
   * @author Egbert vand er Wal
   */
  protected static class HostQueue {
    protected String host;
    protected long nextFetchTime;
    
    public WebURL head;
    public WebURL tail;
    
    public HostQueue(String host) {
      this.host = host;
    }
  }
  
  /**
   * This class is used to compare hosts based on the next fetch time
   * as obtained from the Page Fetcher, their priority and the crawl depth,
   * so give the best candidate for fetching.
   * 
   * @author Egbert van der Wal
   */
  protected class HostComparator implements Comparator<HostQueue> {
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
      
      return lhs.head.compareTo(rhs.head);
    }
  }
  
  /** The mapping between a hostname and its head/tail, used to determine crawl order */
  protected HashMap<String, HostQueue> host_queue = new HashMap<String, HostQueue>();
  
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
    boolean debug = logger.isDebugEnabled();
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
      if (debug)
        logger.trace("Inserting new host queue for host {} starting with docid: ##{}##", host, url.getDocid());
      return result;
    }
    
    int comp;
    
    // Need to insert it in the queue at the appropriate position
    if ((comp = url.compareTo(hq.tail)) >= 0) {
      if (comp == 0)
        throw new RuntimeException("Duplicate URL: " + url.getDocid());
      
      // Tail-insert
      WebURL tail = hq.tail;
      url.setPrevious(tail);
      url.setNext((byte []) null);
      tail.setNext(url);
      
      hq.tail = url;
      result &= crawl_queue_db.update(tail);
      result &= crawl_queue_db.put(url);
      if (debug)
        logger.trace("Doing tail-insert for host {} for URL with docid: {}", host, url.getDocid());
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
      if (debug)
        logger.trace("Doing head-insert for host {} for URL with docid: {}", host, url.getDocid());
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
    if (debug)
      logger.trace("Host {} - inserting new docid {} between {} and {}", host, url.getDocid(), prev.getDocid(), cur.getDocid());
    
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
    logger.debug("Sorting and selecting best URL out of {} hosts took {}ms -> resulting delay: {}ms for host {}", sorted.size(), durr, delay, best.host);
    if (best.nextFetchTime > threshold)
      return null;
    
    WebURL best_url = best.head;
    if (best_url == null)
      throw new RuntimeException("HostQueue head is null. This should not happen!");
    
    // Update the linkedlist in the crawl queue
    crawl_queue_db.removeURL(best_url);
    
    byte[] next_key = best_url.getNext();
    if (next_key == null) {
      // The queue is empty
      host_queue.remove(best.host);
    } else {
      // Move the head to the next element
      if (best.tail.compareKey(next_key) == 0) {
        best.head = best.tail;
      } else {
        best.head = crawl_queue_db.get(next_key);
      }
      
      best.head.setPrevious((byte []) null);
      if (!crawl_queue_db.update(best.head))
        throw new RuntimeException("Could not update URL in database: " + best.head.getURL());
    }
    
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
            if (next == null) {
              host_queue.remove(host);
            }
            else
              hq.head = next;
          }
          
          if (hq.tail.getDocid() == url.getDocid()) {
            if (prev == null) {
              host_queue.remove(host);
            }
            else
              hq.tail = prev;
          }
             
          return IterateAction.REMOVE;
        }
        return IterateAction.CONTINUE;
      }
    });
    logger.debug("Removed {} offspring of seed [[{}]]", num_removed.val, seed_doc_id);
  }
  
  /**
   * Validate the queue - check if the entire queue is traversable, and
   * that head and tail corresponds to the data in the database.
   * 
   * It will also check the other way around: traverse all URLs in the database
   * and check if they have a corresponding HostQueue object.
   * 
   * @return A host for which the queue failed, of * if the count did not match
   *         between the crawl_queue_db and the host_queue. When no problems
   *         were detected, null is returned.
   */
  public String validateQueue() {
    HostQueue hq = null;
    byte [] error_key = null;
    WebURL error_url = null;
    WebURL error_url2 = null;
    
    long st = System.nanoTime();
    int url_counter = 0;
    try {
      for (Map.Entry<String, BerkeleyDBQueue.HostQueue> e : host_queue.entrySet()) {
        hq = e.getValue();

        if (hq.head == null)
          throw new RuntimeException("HostQueue has no head");

        if (hq.tail == null)
          throw new RuntimeException("HostQueue has no tail");

        WebURL cur = crawl_queue_db.get(hq.head.getKey());
        if (cur == null) {
          error_key = hq.head.getKey();
          throw new RuntimeException("HostQueue head is not present in database. Missing docid: " + hq.head.getDocid());
        }

        if (cur.getDocid() != hq.head.getDocid()) {
          error_key = hq.head.getKey();
          error_url = cur;
          throw new RuntimeException("HostQueue head's docid does not match database - db says: " + cur.getDocid() + " - HostQueue says: " + hq.head.getDocid());
        }

        WebURL tail_check = crawl_queue_db.get(hq.tail.getKey());
        if (tail_check == null) {
          error_url = tail_check;
          error_key = hq.tail.getKey();
          throw new RuntimeException("HostQueue tail is not present in database. Missing docid: " + hq.tail.getDocid());
        }

        if (tail_check.getDocid() != hq.tail.getDocid()) {
          error_url = tail_check;
          error_key = hq.tail.getKey();
          throw new RuntimeException("HeadQueue tail's docid does not match database - db says: " + cur.getDocid() + " - HostQueue says: " + hq.head.getDocid());
        }

        WebURL prev = null;
        WebURL next = null;
        int i = 0;
        while (cur != null) {
          ++url_counter;
          byte [] next_key = cur.getNext();
          if (next_key != null) {
            next = crawl_queue_db.get(next_key);
            if (next == null) {
              error_url = cur;
              error_key = next_key;
              throw new RuntimeException("HostQueue #" + i + " - next_key (error_key) is not null for current URL (error_url) but does not exist in database");
            }

            if (cur.getDocid() == hq.tail.getDocid()) {
              error_url = cur;
              error_key = next_key;
              throw new RuntimeException("HostQueue #" + i + " is marked as tail, but next_key (error_key) is not null for URL (error_url)");
            }
          } else {
            next = null;
          }

          byte [] prev_key = cur.getPrevious();
          if (prev_key != null) {
            WebURL tmp_prev = crawl_queue_db.get(prev_key);
            if (tmp_prev == null) {
              error_key = prev_key;
              error_url = cur;
              throw new RuntimeException("HostQueue #" + i + " - URL (error_url) has a prev_key (error_key) that is not present in the database");
            }

            if (prev == null) {
              error_key = prev_key;
              error_url = cur;
              throw new RuntimeException("HostQueue #" + i + " - URL (error_url) has a prev_key (error_key) even though it is the head of the queue");
            }

            if (tmp_prev.getDocid() != prev.getDocid()) {
              error_key = prev_key;
              error_url = cur;
              throw new RuntimeException("HostQueue #" + i + " - URL (error_url) has a prev_key (error_key) but it was reached from docid: " + prev.getDocid());
            }
          } else if (prev != null) {
            error_url = cur;
            error_url2 = prev;
            throw new RuntimeException("HostQueue #" + i + " - URL (error_url) does noth ave a prev_key, but it is not the head of the queue, but was reached from a URL (error_url2)");
          }

          prev = cur;
          cur = next;
          ++i;
        }
        
        if (prev.getDocid() != hq.tail.getDocid()) {
          error_url = hq.tail;
          error_url2 = prev;
          
          throw new RuntimeException("HostQueue has a tail (error_url) that is not the last element reachable through the queue (error_url2).");
          
        }
      }

      final Util.Reference<Integer> url_counter2 = new Util.Reference<Integer>(0);
      crawl_queue_db.iterate(new Processor<WebURL, IterateAction>() {
        @Override
        public IterateAction apply(WebURL url) {
          ++url_counter2.val;
          String host = url.getURI().getHost();
          HostQueue hq = host_queue.get(host);

          if (hq == null)
            throw new RuntimeException("Element in URL queue is not in host_queue - docid: " + url.getDocid());
          return IterateAction.CONTINUE;
        }
      });
      
      if (url_counter != url_counter2.val) {
        logger.error("Traversing HostQueue results in {} URLs, traversing crawl_queue_db results in {} URLs", url_counter, url_counter2.val);
        return "*";
      }
    } catch (RuntimeException e) {
      logger.error("Failed to validate HostQueue for host: {} - Error: {}", hq.host, e.getMessage());
      
      if (error_key != null) {
        logger.error("Error-key: {}", error_key);
      }
      
      if (error_url != null) {
        logger.error(
            "Error-url - URL: {} Docid: {} Seed: {} Priority: {} Depth: {} Key: {}", 
            error_url.getURL(), error_url.getDocid(), error_url.getSeedDocid(),
            error_url.getPriority(), error_url.getDepth(), error_url.getKey()
        );
      }
      
      if (error_url2 != null) {
        logger.error(
            "Error-url2 - URL: {} Docid: {} Seed: {} Priority: {} Depth: {} Key: {}", 
            error_url2.getURL(), error_url2.getDocid(), error_url2.getSeedDocid(), 
            error_url2.getPriority(), error_url2.getDepth(), error_url2.getKey()
        );
      }
      
      showHostQueue(hq);
      return hq.host;
    }
    
    long dur = System.nanoTime() - st;
    logger.info("Validating host_queue took {} ms", Math.round(dur / 10000.0) / 100.0);
    return null;
  }
  
  public void showHostQueue(String host) {
    HostQueue hq = host_queue.get(host);
    if (hq != null)
      showHostQueue(hq);
  }
  /**
   * List a queue to the logger, useful for debugging issues.
   * 
   * @param hq The HostQueue that should be listed
   */
  protected void showHostQueue(HostQueue hq) {
    logger.error("-- Listing queue for host {}", hq.host);
    WebURL cur = hq.head;
    String host = hq.host;
    int i = 0;
    boolean tailFound = false;
    while (cur != null) {
      String special = "";
      if (cur.getDocid() == hq.head.getDocid())
        special += "[HEAD]";
      if (cur.getDocid() == hq.tail.getDocid()) {
        special += "[TAIL]";
        tailFound = true;
      }
      
      logger.error("#{}{}.\tURL: {}\tDocid: {}\tPriority: {}\tDepth: {}", i, special, cur.getURL(), cur.getDocid(), cur.getPriority(), cur.getDepth());
      ++i;
      byte [] next_key = cur.getNext();
      cur = crawl_queue_db.get(next_key);
      if (cur == null && next_key != null) {
        logger.error("-- Error: URL referenced by getNext() could not be found - Key: {}", next_key);
      }
    }
    
    if (!tailFound)
      logger.error("!Tail.\tURL: {}\tDocid: {}\tPriority: {}\tDepth: {}", hq.tail.getURL(), hq.tail.getDocid(), hq.tail.getPriority(), hq.tail.getDepth());
    logger.error("-- Finished listing queue for host {}", host);
  }
}