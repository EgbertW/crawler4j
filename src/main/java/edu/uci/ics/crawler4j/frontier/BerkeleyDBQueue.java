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

import com.sleepycat.je.Environment;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.crawler.exceptions.QueueException;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.frontier.URLQueue.DBTransaction;
import edu.uci.ics.crawler4j.frontier.URLQueue.DBVisitor;
import edu.uci.ics.crawler4j.frontier.URLQueue.TransactionAbort;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.IterateAction;
import edu.uci.ics.crawler4j.util.Util;
import edu.uci.ics.crawler4j.util.Util.Reference;

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
   * @author Egbert van der Wal
   */
  protected static class HostQueue {
    protected String host;
    protected long nextFetchTime;
    protected Long lastAssigned = null;
    protected byte [] host_key;
    
    public WebURL head;
    public WebURL tail;
    
    public HostQueue(String host, WebURL first) {
      this.host = host;
      this.host_key = WebURL.createKey(first.getPriority(), (short)0, first.getSeedDocid());
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
      long long_nft = System.currentTimeMillis() + 900000;
      for (Map.Entry<String, HostQueue> e : host_queue.entrySet()) {
        Long nft = nextFetchTimes.get(e.getKey());
        e.getValue().nextFetchTime = nft == null ? 0 : nft;
        if (nft != null && nft > long_nft) {
          long remain = (nft - System.currentTimeMillis()) / 1000;
          logger.warn("Next fetch time for host {} is more then 15 minutes in the future - {} seconds remaining", e.getKey(), remain);
        }
      }
    }
    
    @Override
    public int compare(HostQueue lhs, HostQueue rhs) {
      if (lhs.nextFetchTime != rhs.nextFetchTime)
        return Long.compare(lhs.nextFetchTime,  rhs.nextFetchTime);
      
      // We compare on the host-key (of the seed), because
      // the elements within the host queue are already
      // ordered using the priority, depth and docid if of the
      // actual URLs.
      return WebURL.compareKey(lhs.host_key, rhs.host_key);
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
    try {
      crawl_queue_db = new URLQueue(env, "CrawlQueue", config.isResumableCrawling());
      in_progress_db = new URLQueue(env, "InProgress", config.isResumableCrawling());
    } catch (TransactionAbort e) {
      throw new RuntimeException("Failed to initialize crawl databases");
    }
    try {
      // All URLs should be in order of their keys, so we can just always
      // do a tail insert
      try {
        crawl_queue_db.iterate(new DBVisitor() {
          @Override
          public IterateAction visit(WebURL url) throws TransactionAbort {
            String host = url.getURI().getHost();
            HostQueue hq = host_queue.get(host);
          
            if (hq == null) {
              hq = new HostQueue(host, url);
              hq.head = hq.tail = url;
              host_queue.put(host, hq);
            } else {
              hq.tail = url;
            }
            return IterateAction.CONTINUE;
          }
        });
      } catch (TransactionAbort e) {
        logger.error("Unable to read contents of database to build HostQueue database");
        throw new RuntimeException(e);
      }
      
      long numPreviouslyInProcessPages = in_progress_db.getLength();
      if (numPreviouslyInProcessPages > 0) {
        // All urls that were in progress need to be added to the queue, so
        // that they may be reassigned to a crawler.
        List<WebURL> urls = in_progress_db.shift((int)numPreviouslyInProcessPages);
        enqueue(urls);
      }
      
      if (crawl_queue_db.getLength() > 0)
        logger.info("Reconstructed crawl queue with {} hosts and {} URLs", host_queue.size(), crawl_queue_db.getLength());
    } catch (TransactionAbort e) {
      logger.error("Error while initializing the crawl queue", e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public boolean enqueue(WebURL url) {
    final boolean debug = logger.isTraceEnabled();
    URI uri = url.getURI();
    String host = uri.getHost();
    
    HostQueue hq = host_queue.get(host);
    boolean result = true;
    if (hq == null) {
      url.setPrevious((byte []) null);
      url.setNext((byte []) null);
      hq = new HostQueue(host, url);
      hq.head = url;
      hq.tail = url;
      
      try {
        result &= crawl_queue_db.put(url);
      } catch (TransactionAbort e) {
        logger.error("Inserting {} into database failed", url, e);
        throw new RuntimeException(e);
      }
      
      if (result)
        result &= host_queue.put(host, hq) == null;
      if (debug)
        logger.trace("Inserting new host queue for host {} starting with {}", host, url);
      return result;
    }
    
    int comp;
    
    // Need to insert it in the queue at the appropriate position
    if ((comp = url.compareTo(hq.tail)) >= 0) {
      if (comp == 0) {
        logger.error("Attempting to insert duplicate URL in queue: {}", url);
        throw new RuntimeException("Duplicate " + url);
      }
      
      // Tail-insert
      final WebURL cur_tail = hq.tail;
      final WebURL to_insert = url;
      final HostQueue hq_ref = hq;
      final Reference<Boolean> success = new Reference<Boolean>(true);
      
      // Update previous tail
      try {
        crawl_queue_db.transaction(new DBTransaction() {
          @Override
          public void run() throws TransactionAbort {
            cur_tail.setNext(to_insert);
            success.val &= crawl_queue_db.update(cur_tail);
            if (!success.val) {
              cur_tail.setNext((byte []) null);
              throw new RuntimeException("Could not update tail of host-queue for host " + hq_ref.host);
            }

            to_insert.setNext((byte []) null);
            to_insert.setPrevious(cur_tail);
            success.val &= crawl_queue_db.put(to_insert);
            if (!success.val) {
              cur_tail.setNext((byte []) null);
              to_insert.setPrevious((byte []) null);
              throw new RuntimeException("Could not insert " + to_insert);
            }
          }
        });
        
        // The database was updated, now update the HostQueue
        hq.tail = to_insert;
        if (debug)
          logger.trace("Performed tail-insert for host {} for {}", host, url);
        return true;
      } catch (TransactionAbort e) {
        logger.error("Transaction aborted", e);
        throw new RuntimeException(e.getCause());
      }
    }
    
    if (url.compareTo(hq.head) < 0) {
      // Head-insert
      final WebURL cur_head = hq.head;
      final WebURL to_insert = url;
      final HostQueue hq_ref = hq;
      final Reference<Boolean> success = new Reference<Boolean>(true);
      
      // Update previous head
      try {
        crawl_queue_db.transaction(new DBTransaction() {
          @Override
          public void run() throws TransactionAbort {
            cur_head.setPrevious(to_insert);
            success.val &= crawl_queue_db.update(cur_head);
            if (!success.val) {
              cur_head.setPrevious((byte []) null);
              throw new RuntimeException("Could not update head of host-queue for host " + hq_ref.host);
            }

            to_insert.setPrevious((byte []) null);
            to_insert.setNext(cur_head);
            success.val &= crawl_queue_db.put(to_insert);
            if (!success.val) {
              cur_head.setPrevious((byte []) null);
              to_insert.setNext((byte []) null);
              throw new RuntimeException("Could not insert " + to_insert);
            }
          }
        });
        
        // The database was updated, now update the HostQueue
        hq_ref.head = to_insert;
        if (debug)
          logger.trace("Performed head-insert for host {} for {}", host, url);
        return true;
      } catch (TransactionAbort e) {
        logger.error("Transaction aborted", e);
        throw new RuntimeException(e.getCause());
      }
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
      try {
        cur = crawl_queue_db.get(next_key);
      } catch (TransactionAbort e) {
        throw new RuntimeException(e);
      }
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
    
    final WebURL tr_prev = prev;
    final WebURL tr_ins = url;
    final WebURL tr_next = cur;
    final HostQueue hq_ref = hq;
    
    try {
      crawl_queue_db.transaction(new DBTransaction() { 
        @Override
        public void run() throws TransactionAbort {
          // Update the URL that preceeds the to-insert URL
          tr_prev.setNext(tr_ins);
          boolean result = crawl_queue_db.update(tr_prev);
          if (result == false) {
            tr_prev.setNext(tr_next);
            throw new RuntimeException("Could not update 'prev-URL' in crawl_queue: " + tr_prev);
          }


          // Update the URL that follows the to-insert URL
          tr_next.setPrevious(tr_ins);
          result = crawl_queue_db.update(tr_next);
          if (result == false) {
            tr_prev.setNext(tr_next);
            tr_next.setPrevious(tr_prev);
            throw new RuntimeException("Could not update 'next-URL' in crawl_queue: " + tr_next);
          }

          // Insert the new URL
          tr_ins.setPrevious(tr_prev);
          tr_ins.setNext(tr_next);
          result = crawl_queue_db.put(tr_ins);
          if (result == false)
            throw new RuntimeException("Could not insert new URL into crawl_queue: " + tr_ins);

          if (debug)
            logger.trace("Host {} - inserting new {} between {} and {}", hq_ref.host, tr_ins, tr_prev, tr_next);
        }
      });
    } catch (TransactionAbort e) {
      logger.error("Transaction aborted", e);
      throw new RuntimeException(e.getCause());
    }
    
    return result;
  }
  
  @Override
  public List<WebURL> enqueue(Collection<WebURL> urls) {
    ArrayList<WebURL> rejects = new ArrayList<WebURL>();
    
    boolean debug = logger.isTraceEnabled();
    
    HashMap<String, Set<WebURL>> new_urls = new HashMap<String, Set<WebURL>>();
    
    logger.debug("Multi-enqueue: inserting {} URLs into crawl queue", urls.size());
    
    // Sort the URLs into their hosts, and for each host, sort on priority
    for (WebURL url : urls) {
      String host = url.getURI().getHost();
      if (!new_urls.containsKey(host))
        new_urls.put(host, new TreeSet<WebURL>());
      new_urls.get(host).add(url);
      if (debug)
        logger.trace("Multi-enqueue: sorting {} into list for host {} - queue size: {}", url, host, new_urls.get(host).size());
    }
    
    for (Map.Entry<String, Set<WebURL>> e : new_urls.entrySet()) {
      String host = e.getKey();
      Set<WebURL> host_urls = e.getValue();
      HostQueue hq = host_queue.get(host);
      
      if (debug)
        logger.trace("Inserting {} URLs for host {}", host_urls.size(), host);
      
      if (hq == null) {
        hq = new HostQueue(host, host_urls.iterator().next());
        hq.head = hq.tail = null;
        host_queue.put(host, hq);
        
        if (debug)
          logger.trace("Multi-enqueue: inserting new HostQueue for host {}", host);
      }
      
      WebURL cursor = hq.head;
      WebURL prev = null;
      
      Set<WebURL> urls_to_insert = new HashSet<WebURL>();
      Set<WebURL> urls_to_update = new HashSet<WebURL>();
      
      Iterator<WebURL> new_iter = host_urls.iterator();
      int comparison;
      while (new_iter.hasNext()) {
        WebURL to_insert = new_iter.next();
        logger.trace("Multi-enqueue: processing {} for host {}", to_insert, host);
        
        comparison = -1;
        // Advance to the point where to where the new URL should be inserted before it
        while (cursor != null && (comparison = to_insert.compareTo(cursor)) >= 0) {
          if (comparison == 0)
            break;
          
          prev = cursor;
          byte [] next_key = cursor.getNext();
          if (hq.tail != null && next_key != null && WebURL.compareKey(hq.tail.getKey(), next_key) == 0) {
            cursor = hq.tail;
          } else {
            try {
              cursor = crawl_queue_db.get(next_key);
            } catch (TransactionAbort ex) {
              throw new RuntimeException(ex);
            }
          }
        }
        
        if (comparison == 0) { 
          logger.warn("Multi-enqueue: rejecting duplicate {}", to_insert);
          rejects.add(to_insert);
          continue;
        }
        
        // At this point, either cursor == null, meaning we do a tail insert, or
        // to_insert.compareTo(cursor) < 0, meaning the new URL should be inserted
        // before the current URL
        if (hq.head == null) {
          to_insert.setPrevious((byte []) null);
          to_insert.setNext((byte []) null);
          hq.head = hq.tail = to_insert;
          if (debug)
            logger.trace("Multi-enqueue: adding first URL to queue for host {}: {}", host, to_insert);
        } else if (cursor == null) { // Tail insert -> prev == hq.tail
          hq.tail.setNext(to_insert);
          to_insert.setPrevious(hq.tail);
          to_insert.setNext((byte []) null);
          if (!urls_to_insert.contains(hq.tail))
            urls_to_update.add(hq.tail);
          
          hq.tail = to_insert;
          if (debug)
            logger.trace("Multi-enqueue: doing tail-insert to queue for host {}: {}", host, to_insert);
        } else if (prev == null) { // Head insert, cursor == hq.head
          to_insert.setPrevious((byte []) null);
          to_insert.setNext(hq.head);
          
          hq.head.setPrevious(to_insert);
          if (!urls_to_insert.contains(hq.head))
            urls_to_update.add(hq.head);
          
          hq.head = to_insert;
          if (debug)
            logger.trace("Multi-enqueue: doing head-insert to queue for host {}: {}", host, to_insert);
        } else { // In between insert prev and cur
          prev.setNext(to_insert);
          to_insert.setPrevious(prev);
          to_insert.setNext(cursor);
          cursor.setPrevious(to_insert);
          
          if (!urls_to_insert.contains(prev))
            urls_to_update.add(prev);
          if (!urls_to_insert.contains(cursor))
            urls_to_update.add(cursor);
          
          if (debug)
            logger.trace("Multi-enqueue: inserting into queue for host {}: {} between {} and {}", host, to_insert, prev, cursor);
        }
        
        // Any following URLs will come after this URL
        prev = to_insert;
        
        // Make sure to insert the new URL
        urls_to_insert.add(to_insert);
        new_iter.remove();
      }
      
      try {
        // Insert all new urls in the database
        crawl_queue_db.put(urls_to_insert);

        // Update all previously known urls
        crawl_queue_db.update(urls_to_update);
      } catch (TransactionAbort ex) {
        throw new RuntimeException(ex);
      }
    }
    
    if (!rejects.isEmpty())
      logger.info("Rejected {} URLs out of {}", rejects.size(), urls.size());
    return rejects;
  }

  @Override
  public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) throws QueueException {
    this.fetcher = fetcher;
    WebURL oldURL = getAssignedURL(crawler);
    if (oldURL != null)
      throw new QueueException(crawler + " has not finished its previous URL: " + oldURL, oldURL);
    
    boolean debug = logger.isTraceEnabled();
    if (host_queue.isEmpty()) {
      if (debug)
        logger.trace("No URLs in the queue at this moment");
      return null;
    }
      
    long start = System.nanoTime();
    // Sort the available hosts based on priority and delay
    TreeSet<HostQueue> sorted = new TreeSet<HostQueue>(new HostComparator(fetcher.getHostMap()));
    sorted.addAll(host_queue.values());
    
    // The head of the list is the best candidate
    HostQueue best = sorted.first();
    long threshold = System.currentTimeMillis() + config.getPolitenessDelay();
    long delay = Math.max(0, best.nextFetchTime - System.currentTimeMillis());
    
    if (debug) {
      long dur = System.nanoTime() - start;
      double durr = Math.round(dur / 1000.0) / 1000.0;
      logger.trace("Sorting and selecting best URL out of {} hosts took {}ms -> resulting delay: {}ms for host {}", sorted.size(), durr, delay, best.host);
    }
    
    if (best.nextFetchTime > threshold)
      return null;
    
    WebURL best_url = best.head;
    if (best_url == null)
      throw new RuntimeException("HostQueue head is null. This should not happen!");
    
    // Update the linkedlist in the crawl queue
    try {
      crawl_queue_db.removeURL(best_url);
      logger.trace("Removed {} from crawl_queue_db", best_url);
    } catch (TransactionAbort e) {
      logger.error("DatabaseError while removing {}", best_url, e);
      throw new QueueException("Could not remove selected URL from crawl_queue", best_url);
    }
    byte[] next_key = best_url.getNext();
    if (next_key == null) {
      // The queue is empty
      if (debug)
        logger.trace("Removing host queue for host {} because the queue is empty", best.host);
      
      host_queue.remove(best.host);
    } else {
      // Move the head to the next element
      if (WebURL.compareKey(best.tail.getKey(), next_key) == 0) {
        logger.trace("After popping head for host {}, one element remains: {}", best.host, best.tail);
        best.head = best.tail;
      } else {
        try {
          best.head = crawl_queue_db.get(next_key);
          logger.trace("Advancing head of queue for host {} to next element: {}", best.host, best.head);
        } catch (TransactionAbort e) {
          throw new RuntimeException(e);
        }
      }
      
      best.head.setPrevious((byte []) null);
      try {
        if (!crawl_queue_db.update(best.head))
          throw new RuntimeException("Could not update URL in database: " + best.head);
      } catch (TransactionAbort e) {
        throw new RuntimeException(e);
      }
    }
    
    if (debug)
      logger.trace("Assigning best {} to crawler: {}", best_url, crawler);
    
    try {
      assign(best_url, crawler);
      in_progress_db.put(best_url);
      fetcher.select(best_url);
      best.lastAssigned = System.currentTimeMillis();
      return best_url;
    } catch (TransactionAbort e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void abandon(WebCrawler crawler, WebURL url) throws QueueException {
    logger.info("{} abandons {}", crawler, url);
    unassign(url, crawler);
    
    try {
      in_progress_db.removeURL(url);
    } catch (TransactionAbort e) {
      throw new QueueException("Failed to remove URL from in_progress_db", url);
    }
    fetcher.unselect(url);
    enqueue(url);
  }

  @Override
  public void setFinishedURL(WebCrawler crawler, WebURL url) throws QueueException {
    unassign(url, crawler);
    try {
      in_progress_db.removeURL(url);
      logger.trace("Removed {} from in_progress_db", url);
    } catch (TransactionAbort e) {
      logger.error("Could not remove {} from in_progress_db", url, e);
      throw new QueueException("Failed to remove URL from in_progress_db", url);
    }
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
  public void removeOffspring(final long seed_doc_id) {
    logger.trace("Removing all offspring for {}", seed_doc_id);
    final Util.Reference<Integer> num_removed = new Util.Reference<Integer>(0);
        try {
      crawl_queue_db.iterate(new DBVisitor() {
        @Override
        public IterateAction visit(WebURL url) throws TransactionAbort {
          if (url.getSeedDocid() == seed_doc_id) {
            logger.trace("Found match for seed_doc_id {}: {}", seed_doc_id, url);
            ++num_removed.val;

            String host = url.getURI().getHost();
            HostQueue hq = host_queue.get(host);

            if (hq == null) {
              logger.error("Element in URL queue is not in host_queue: {} (docid: {})", url.getURL(), url.getDocid());
              logger.error("Walking backwards up to head: ");
              int rc = 0;
              WebURL cur = url;
              while (cur.getPrevious() != null) {
                ++rc;
                WebURL prev = null;
                prev = crawl_queue_db.get(cur.getPrevious());
                
                if (prev != null) {
                  logger.error("{} step(s) backward: {} (docid: {}, seed: {}, parent: {}) - prev: {} next: {}", rc, prev.getURL(), prev.getDocid(), prev.getSeedDocid(), prev.getParentDocid(), prev.getPrevious(), prev.getNext());
                } else {
                  logger.error("{} step(s) backward: {} (docid: {})", rc, cur.getPrevious(), Util.extractLongFromByteArray(cur.getPrevious(), 2));
                  break;
                }
                cur = prev;
              }
              
              logger.error("Walking forwards up to tail: ");
              rc = 0;
              cur = url;
              while (cur.getNext() != null) {
                ++rc;
                WebURL next = null;
                next = crawl_queue_db.get(cur.getNext());
                
                if (next != null) {
                  logger.error("{} step(s) forward: {} (docid: {}, seed: {}, parent: {}) - prev: {} next: {}", rc, next.getURL(), next.getDocid(), next.getSeedDocid(), next.getParentDocid(), next.getPrevious(), next.getNext());
                } else {
                  logger.error("{} step(s) forward: {} (docid: {})", rc, cur.getNext(), Util.extractLongFromByteArray(cur.getNext(), 2));
                  break;
                }
                cur = next;
              }
              logger.error("Done tracking URL queue. Ending now");
              
              throw new RuntimeException("Element in URL queue is not in host_queue - docid: " + url.getDocid() + " host: " + host);
            }

            WebURL prev = crawl_queue_db.get(url.getPrevious());
            WebURL next = crawl_queue_db.get(url.getNext());

            if (prev != null && prev.getDocid() == hq.head.getDocid()) {
              prev = hq.head;
              logger.trace("Element before {} is head of queue for host {}: {}", url, hq.host, prev);
            }

            if (next != null && next.getDocid() == hq.tail.getDocid()) {
              next = hq.tail;
              logger.trace("Element before {} is tail of queue for host {}: {}", url, hq.host, next);
            }

            if (prev != null) {
              prev.setNext(next);
              logger.trace("Updating {} to preceed {}", prev, next);
              if (!crawl_queue_db.update(prev))
                throw new RuntimeException("Failed to update " + prev);
            }

            if (next != null) {
              next.setPrevious(prev);
              logger.trace("Updating {} to follow {}", next, prev);
              try {
                if (!crawl_queue_db.update(next))
                  throw new RuntimeException("Failed to update " + next);
              } catch (TransactionAbort e) {
                throw new RuntimeException(e);
              }
            }

            if (hq.head.getDocid() == url.getDocid()) {
              if (next == null) {
                logger.trace("Removed {} was the only element in queue for host {} - removing queue", url, hq.host);
                host_queue.remove(host);
              } else {
                hq.head = next;
                logger.trace("Removed {}  was head of queue for host {} - new head: {}", url, hq.host, hq.head);
              }
            } else if (hq.tail.getDocid() == url.getDocid()) {
              // prev must be non-null, because otherwise head == tail, and
              // we would've handled it before
              hq.tail = prev;
              logger.trace("Removed {} was tail of queue for host {} - new tail: {}", url, hq.host, hq.tail);
            }

            return IterateAction.REMOVE;
          }
          return IterateAction.CONTINUE;
        }
      });
    } catch (TransactionAbort e) {
      logger.error("Removing offspring of seed {} failed, stacktrace follows", seed_doc_id, e);
      throw new RuntimeException("Iteration failed while removing offspring of seed " + seed_doc_id);
    }
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
          throw new RuntimeException("HostQueue head is not present in database. Missing " + hq.head);
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
          throw new RuntimeException("HostQueue tail is not present in database. Missing " + hq.tail);
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
      try {
        crawl_queue_db.iterate(new DBVisitor() {
          @Override
          public IterateAction visit(WebURL url) throws TransactionAbort {
            ++url_counter2.val;
            String host = url.getURI().getHost();
            HostQueue hq = host_queue.get(host);

            if (hq == null)
              throw new RuntimeException("Element in URL queue is not in host_queue: " + url);
            return IterateAction.CONTINUE;
          }
        });
      } catch (TransactionAbort e) {
        logger.error("Failed to validate URL queue due to database error", e);
        return "*";
      }
      
      if (url_counter != url_counter2.val) {
        logger.error("Traversing HostQueue results in {} URLs, traversing crawl_queue_db results in {} URLs", url_counter, url_counter2.val);
        WebURL bad_url = null;
        try {
          bad_url = crawl_queue_db.iterate(new DBVisitor() {
            @Override
            public IterateAction visit(WebURL url) throws TransactionAbort {
              System.out.println("Visiting " + url);
              String host = url.getURI().getHost();
              HostQueue hq = host_queue.get(host);
              if (hq == null) {
                logger.error("{} exists in crawl_queue_db but does not have a HostQueue entry", url);
                return IterateAction.RETURN;
              }

              WebURL cursor = hq.head;
              while (cursor != null) {
                if (cursor.equals(url))
                  return IterateAction.CONTINUE; // URL is reachable
                
                byte [] next_key = cursor.getNext();
                cursor = crawl_queue_db.get(next_key);
              }

              // We reached the end of the queue, but did not encounter
              // the visited URL, so it's unreachable.
              return IterateAction.RETURN;
            }
          });
        } catch (TransactionAbort e) {
          logger.error("Failed to validate URL queue due to database error", e);
        }
        if (bad_url != null)
          return bad_url.getURI().getHost();
        return "*";
      }
    } catch (RuntimeException e) {
      logger.error("Failed to validate HostQueue for host: {} - Error: {}", hq.host, e.getMessage());
      
      if (error_key != null) {
        logger.error("Error-key: {}", error_key);
      }
      
      if (error_url != null) {
        logger.error("Error-url - ", error_url);
      }
      
      if (error_url2 != null) {
        logger.error("Error-url2 - {}", error_url2);
      }
      
      showHostQueue(hq);
      return hq.host;
    } catch (TransactionAbort e) {
      logger.error("Transaction was aborted in queue unexpectedly", e);
    }
    
    long dur = System.nanoTime() - st;
    logger.info("Validating host_queue took {} ms", Math.round(dur / 10000.0) / 100.0);
    return null;
  }
  
  public void showHostQueue(String host) {
    HostQueue hq = host_queue.get(host);
    if (hq != null)
      showHostQueue(hq);
    else
      logger.error("-- No Host Queue for host {}", host);;
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
      
      logger.error("#{}{}.\t{}", i, special, cur);
      ++i;
      byte [] next_key = cur.getNext();
      try {
        cur = crawl_queue_db.get(next_key);
      } catch (TransactionAbort e) {
        throw new RuntimeException(e);
      }
      if (cur == null && next_key != null) {
        logger.error("-- Error: URL referenced by getNext() could not be found - Key: {}", next_key);
      }
    }
    
    if (!tailFound)
      logger.error("!Tail.\t{}", hq.tail);
    logger.error("-- Finished listing queue for host {}", host);
  }
}
