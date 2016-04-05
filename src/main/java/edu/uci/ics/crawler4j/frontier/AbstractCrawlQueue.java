package edu.uci.ics.crawler4j.frontier;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.url.WebURL;

public abstract class AbstractCrawlQueue implements CrawlQueue {
  protected CrawlConfig config;
  protected String last_error = null;
  
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
}
