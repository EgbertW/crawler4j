package edu.uci.ics.crawler4j.crawler.exceptions;

/**
 * Created by Avi Hayun on 12/8/2014.
 * Thrown when trying to fetch a page which is bigger than allowed size
 */
public class PageBiggerThanMaxSizeException extends Exception {
  private static final long serialVersionUID = -8083758511548344680L;
  long pageSize;

  public PageBiggerThanMaxSizeException(long pageSize) {
    super("Aborted fetching of this URL as it's size ( " + pageSize + " ) exceeds the maximum size");
    this.pageSize = pageSize;
  }

  public long getPageSize() {
    return pageSize;
  }
}