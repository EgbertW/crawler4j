package edu.uci.ics.crawler4j.crawler.exceptions;

import edu.uci.ics.crawler4j.url.WebURL;

public class QueueException extends Exception {
  protected WebURL url;
  
  public QueueException(String string, WebURL url) {
    super(string);
    this.url = url;
  }

  private static final long serialVersionUID = 2404845779063866069L;
  
  public WebURL getURL() {
    return this.url;
  }
}
