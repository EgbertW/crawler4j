package edu.uci.ics.crawler4j.frontier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;

public abstract class AbstractCrawlQueue implements CrawlQueue {
  protected CrawlConfig config;
  protected String last_error = null;
  
  /** The URLs that have been assigned to a crawler */
  protected HashMap<Long, WebURL> urls_in_progress = new HashMap<Long, WebURL>();
  
  @Override
  public void setCrawlConfiguration(CrawlConfig config) {
    this.config = config;
  }

  @Override
  public List<WebURL> enqueue(List<WebURL> urls) {
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

  protected void assign(WebURL url, WebCrawler crawler) {
    WebURL prev = urls_in_progress.put(crawler.getThread().getId(), url);
    if (prev != null) {
      urls_in_progress.remove(crawler.getThread().getId());
      throw new RuntimeException("Crawler " + crawler.getThread().getId() + " was assigned "
          + " URL " + prev.getURL() + "(" + prev.getDocid() + "), cannot assign a new one");
    }
  }
  
  protected void unassign(WebURL url, WebCrawler crawler) {
    WebURL prev = urls_in_progress.put(crawler.getThread().getId(), url);
    if (prev == null) {
      throw new RuntimeException("Crawler " + crawler.getThread().getId() + " had no assigned URL "
          + " - cannot unassign " + url.getURL() + " (" + url.getDocid() + ")");
    }
    if (prev.getDocid() != url.getDocid()) {
      throw new RuntimeException("Crawler " + crawler.getThread().getId() + " was assigned URL "
          + prev.getURL() + " (" + prev.getDocid() + ")"
          + " - cannot unassign " + url.getURL() + " (" + url.getDocid() + ")");
    }
  }
}