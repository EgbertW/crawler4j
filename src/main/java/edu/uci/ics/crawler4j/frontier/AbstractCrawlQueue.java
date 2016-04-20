package edu.uci.ics.crawler4j.frontier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public abstract class AbstractCrawlQueue implements CrawlQueue {
  protected Logger logger = LoggerFactory.getLogger(this.getClass());
  
  protected CrawlConfig config;
  protected String last_error = null;
  
  /** The URLs that have been assigned to a crawler */
  private HashMap<Long, WebURL> urls_in_progress = new HashMap<Long, WebURL>();
  
  @Override
  public void setCrawlConfiguration(CrawlConfig config) {
    this.config = config;
  }

  @Override
  public List<WebURL> enqueue(Collection<WebURL> urls) {
    ArrayList<WebURL> rejects = new ArrayList<WebURL>();
    for (WebURL url : urls) {
      try {
        enqueue(url);
      } catch (RuntimeException e) {
        rejects.add(url);
        last_error = e.getMessage();
      }
    }
    
    return rejects;
  }

  @Override
  public String getLastError() {
    return last_error;
  }
  
  @Override
  public long getNumInProgress() {
    return urls_in_progress.size();
  }

  protected WebURL getAssignedURL(WebCrawler crawler) {
    return urls_in_progress.get(crawler.getId());
  }
  
  protected void assign(WebURL url, WebCrawler crawler) {
    WebURL prev = urls_in_progress.put(crawler.getId(), url);
    if (prev != null) {
      urls_in_progress.remove(crawler.getThread().getId());
      throw new RuntimeException("Crawler " + crawler.getId() + " was assigned "
          + " URL " + prev.getURL() + "(" + prev.getDocid() + "), cannot assign a new one");
    }
    logger.debug("Assigning URL {} ({}) to crawler {}", url.getURL(), url.getDocid(), crawler.getMyId());
  }
  
  protected void unassign(WebURL url, WebCrawler crawler) {
    WebURL prev = urls_in_progress.put(crawler.getId(), url);
    if (prev == null) {
      throw new RuntimeException("Crawler " + crawler.getThread().getId() + " had no assigned URL "
          + " - cannot unassign " + url.getURL() + " (" + url.getDocid() + ")");
    }
    if (prev.getDocid() != url.getDocid()) {
      throw new RuntimeException("Crawler " + crawler.getThread().getId() + " was assigned URL "
          + prev.getURL() + " (" + prev.getDocid() + ")"
          + " - cannot unassign " + url.getURL() + " (" + url.getDocid() + ")");
    }
    urls_in_progress.remove(crawler.getId());
    logger.debug("Removing assignment of URL {} ({}) to crawler {}", url.getURL(), url.getDocid(), crawler.getMyId());
  }
  
  @Override
  public WebURL reassign(Thread oldthread, Thread newthread) {
    WebURL prev = urls_in_progress.get(oldthread.getId());
    
    if (prev != null) {
      urls_in_progress.remove(oldthread.getId());
      urls_in_progress.put(newthread.getId(), prev);
    }
    return prev;
  }
}