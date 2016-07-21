/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.crawler4j.crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.http.HttpStatus;
import org.apache.http.impl.EnglishReasonPhraseCatalog;

import edu.uci.ics.crawler4j.crawler.exceptions.ContentFetchException;
import edu.uci.ics.crawler4j.crawler.exceptions.PageBiggerThanMaxSizeException;
import edu.uci.ics.crawler4j.crawler.exceptions.ParseException;
import edu.uci.ics.crawler4j.crawler.exceptions.QueueException;
import edu.uci.ics.crawler4j.crawler.exceptions.RedirectException;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.frontier.DocIDServer;
import edu.uci.ics.crawler4j.frontier.Frontier;
import edu.uci.ics.crawler4j.parser.NotAllowedContentException;
import edu.uci.ics.crawler4j.parser.ParseData;
import edu.uci.ics.crawler4j.parser.Parser;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;
import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jext.Logger;
import uk.org.lidalia.slf4jext.LoggerFactory;

/**
 * WebCrawler class in the Runnable class that is executed by each crawler thread.
 *
 * @author Yasser Ganjisaffar
 */
public class WebCrawler implements Runnable {

  protected static final Logger logger = LoggerFactory.getLogger(WebCrawler.class);

  /**
   * The id associated to the crawler thread running this instance
   */
  protected int myId;

  /**
   * The controller instance that has created this crawler thread. This
   * reference to the controller can be used for getting configurations of the
   * current crawl or adding new seeds during runtime.
   */
  protected CrawlController myController;

  /**
   * The thread within which this crawler instance is running.
   */
  private Thread myThread;

  /**
   * The parser that is used by this crawler instance to parse the content of the fetched pages.
   */
  private Parser parser;

  /**
   * The fetcher that is used by this crawler instance to fetch the content of pages from the web.
   */
  private PageFetcher pageFetcher;

  /**
   * The RobotstxtServer instance that is used by this crawler instance to
   * determine whether the crawler is allowed to crawl the content of each page.
   */
  private RobotstxtServer robotstxtServer;

  /**
   * The DocIDServer that is used by this crawler instance to map each URL to a unique docid.
   */
  private DocIDServer docIdServer;

  /**
   * The Frontier object that manages the crawl queue.
   */
  private Frontier frontier;

  /**
   * Is the current crawler instance waiting for new URLs? This field is
   * mainly used by the controller to detect whether all of the crawler
   * instances are waiting for new URLs and therefore there is no more work
   * and crawling can be stopped.
   */
  private boolean isWaitingForNewURLs;

  /** 
   * The queue of URLs for this crawler instance
   */
  private WebURL assignedURL = null;
  
  /**
   * Initializes the current instance of the crawler
   *
   * @param id
   *            the id of this crawler instance
   * @param crawlController
   *            the controller that manages this crawling session
   */
  public void init(int id, CrawlController crawlController) {
    this.myId = id;
    this.pageFetcher = crawlController.getPageFetcher();
    this.robotstxtServer = crawlController.getRobotstxtServer();
    this.docIdServer = crawlController.getDocIdServer();
    this.frontier = crawlController.getFrontier();
    this.parser = new Parser(crawlController.getConfig());
    this.myController = crawlController;
    this.isWaitingForNewURLs = false;
  }

  /**
   * Get the id of the current crawler instance
   *
   * @return the id of the current crawler instance
   */
  public int getMyId() {
    return myId;
  }
  
  /**
   * Only to be use from tests
   * 
   * @param id The ID to set
   */
  public void setMyId(int id) {
    this.myId = id;
  }

  public CrawlController getMyController() {
    return myController;
  }

  /**
   * This function is called just before starting the crawl by this crawler
   * instance. It can be used for setting up the data structures or
   * initializations needed by this crawler instance.
   */
  public void onStart() {
    // Do nothing by default
    // Sub-classed can override this to add their custom functionality
  }

  /**
   * This function is called just before the termination of the current
   * crawler instance. It can be used for persisting in-memory data or other
   * finalization tasks.
   */
  public void onBeforeExit() {
    // Do nothing by default
    // Sub-classed can override this to add their custom functionality
  }

  /**
   * This function is called once the header of a page is fetched. It can be
   * overridden by sub-classes to perform custom logic for different status
   * codes. For example, 404 pages can be logged, etc.
   *
   * @param webUrl WebUrl containing the statusCode
   * @param statusCode Html Status Code number
   * @param statusDescription Html Status COde description
   */
  protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
    // Do nothing by default
    // Sub-classed can override this to add their custom functionality
  }

  /**
   * This function is called before processing of the page's URL
   * It can be overridden by subclasses for tweaking of the url before processing it.
   * For example, http://abc.com/def?a=123 - http://abc.com/def
   *
   * @param curURL current URL which can be tweaked before processing
   * @return tweaked WebURL
   */
  protected WebURL handleUrlBeforeProcess(WebURL curURL) {
    return curURL;
  }

  /**
   * This function is called if the content of a url is bigger than allowed size.
   *
   * @param urlStr - The URL which it's content is bigger than allowed size
   * @param pageSize The actual page size
   */
  protected void onPageBiggerThanMaxSize(String urlStr, long pageSize) {
    logger.warn("Skipping a URL: {} which was bigger ( {} ) than max allowed size", urlStr, pageSize);
  }

  /**
   * This function is called if the crawler encountered an unexpected http status code ( a status code other than 3xx)
   *
   * @param urlStr URL in which an unexpected error was encountered while crawling
   * @param statusCode Html StatusCode
   * @param contentType Type of Content
   * @param description Error Description
   */
  protected void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType, String description) {
    logger.warn("Skipping URL: {}, StatusCode: {}, {}, {}", urlStr, statusCode, contentType, description);
    // Do nothing by default (except basic logging)
    // Sub-classed can override this to add their custom functionality
  }

  /**
   * This function is called if the content of a url could not be fetched.
   *
   * @param webUrl URL which content failed to be fetched
   */
  protected void onContentFetchError(WebURL webUrl) {
    logger.warn("Can't fetch content of: {}", webUrl.getURL());
    // Do nothing by default (except basic logging)
    // Sub-classed can override this to add their custom functionality
  }
  
  /**
   * This function is called when a unhandled exception was encountered during fetching
   *
   * @param webUrl URL where a unhandled exception occured
   * @param e The exception that was not handled
   */
  protected void onUnhandledException(WebURL webUrl, Throwable e) {
    logger.warn("Unhandled exception while fetching {}: {}", webUrl.getURL(), e.getMessage());
    logger.info("Stacktrace: ", e);
    // Do nothing by default (except basic logging)
    // Sub-classed can override this to add their custom functionality
  }
  
  /**
   * This function is called if there has been an error in parsing the content.
   *
   * @param webUrl URL which failed on parsing
   */
  protected void onParseError(WebURL webUrl) {
    logger.warn("Parsing error of: {}", webUrl.getURL());
    // Do nothing by default (Except logging)
    // Sub-classed can override this to add their custom functionality
  }

  /**
   * The CrawlController instance that has created this crawler instance will
   * call this function just before terminating this crawler thread. Classes
   * that extend WebCrawler can override this function to pass their local
   * data to their controller. The controller then puts these local data in a
   * List that can then be used for processing the local data of crawlers (if needed).
   *
   * @return currently NULL
   */
  public Object getMyLocalData() {
    return null;
  }
  
  /**
   * Replace the assigned URL with null, and return the current URL.
   * This may be called by the CrawlController to relocate URLs to a newly
   * started thread in case of a crash. Should only be called when the thread is dead.
   * 
   * @return The list of assigned URLs
   */
  public WebURL extractAssignedURL() {
      WebURL cur = assignedURL;
      assignedURL = null;
      return cur;
  }
  
  /**
   * Set a URL as the assigned URL. This should be used
   * with the result of extractAssignedURL from a different instance.
   * 
   * @param url The previously assigned URL to assign to this instance
   */
  public void resume(WebURL url) {
      assignedURL = url;
  }

  @Override
  public void run() {
    onStart();
    while (true) {
      if (assignedURL == null) {
        isWaitingForNewURLs = true;
        try {
          assignedURL = frontier.getNextURL(this, pageFetcher);
        } catch (QueueException e) {
          WebURL url = e.getURL();
          if (url == null)
            logger.error("Could not obtain new URL as the frontier thinks that URL <null> was already assigned. Something went terribly wrong.");
          else
            logger.error("Could not obtain new URL as the frontier thinks that URL {} (docid: {}) was already assigned. Something went wrong.", url.getURL(), url.getDocid());
          myController.shutdown();
        }
        isWaitingForNewURLs = false;
      }
      if (assignedURL != null) {
        // We should be extremely cautious with external elements messing
        // with the URL as we need to remove it from the queue after processing.
        // Therefore, a full copy is made before passing it along.
        WebURL backup = new WebURL(assignedURL);
          
        try {
          WebURL fetchURL = handleUrlBeforeProcess(assignedURL);
          
          // Unselect the URL in the page fetcher if it's not going to be crawled or if the crawler changed it
          // to a different host
          if (fetchURL == null || !fetchURL.getURI().getHost().equals(assignedURL.getURI().getHost()))
            pageFetcher.unselect(assignedURL);

          if (fetchURL != null && !fetchURL.getSeedEnded())
            processPage(fetchURL);
        } catch (OutOfMemoryError e) {
          logger.error("OutOfMemory occured while processing URL {}. Shutting down.", assignedURL.getURL());
          logger.error("Stacktrace for OOM", e);
          myController.shutdown();
          throw e;
        } finally {
          // Handle the finishing of the URL in the finally clause
          // to make sure it is ALWAYS executed, no matter what.
          try {
            frontier.setProcessed(this, backup);
          } catch (QueueException e) {
            logger.error("Could not set processed on URL {} (docid: {}) because frontier doesn't think it's assigned to me", backup.getURL(), backup.getDocid());
            logger.error("Stacktrace", e);
            myController.shutdown();
          }
        }
        if (myController.isShuttingDown()) {
          logger.info("Exiting because of controller shutdown.");
          return;
        }
        assignedURL = null;
      }
    }
  }

  /**
   * Classes that extends WebCrawler should overwrite this function to tell the
   * crawler whether the given url should be crawled or not. The following
   * default implementation indicates that all urls should be included in the crawl.
   *
   * @param url
   *            the url which we are interested to know whether it should be
   *            included in the crawl or not.
   * @param referringPage
   *           The Page in which this url was found.
   * @return if the url should be included in the crawl it returns true,
   *         otherwise false is returned.
   */
  public boolean shouldVisit(Page referringPage, WebURL url) {
    // By default allow all urls to be crawled.
    return true;
  }

  /**
   * Classes that extends WebCrawler should overwrite this function to process
   * the content of the fetched and parsed page.
   *
   * @param page
   *            the page object that is just fetched and parsed.
   */
  public void visit(Page page) {
    // Do nothing by default
    // Sub-classed should override this to add their custom functionality
  }

  /**
   * Classes that extend WebCrawler can overwrite this function to perform
   * an action when the last page resulting from a seed has been processed.
   * 
   * @param l The document ID of the seed
   */
  public void handleSeedEnd(long l) {
      // Do nothing by default
      // Sub-classes can override this to add their custom functionality
  }

  private void processPage(WebURL curURL) {
    PageFetchResult fetchResult = null;
    try {
      if (curURL == null) {
        throw new Exception("Failed processing a NULL url !?");
      }

      fetchResult = pageFetcher.fetchPage(curURL);
      int statusCode = fetchResult.getStatusCode();
      String reasonStatusCode;
      try {
        reasonStatusCode = EnglishReasonPhraseCatalog.INSTANCE.getReason(statusCode, Locale.ENGLISH);
      } catch (IllegalArgumentException e) {
        String reason = fetchResult.getStatusReason();
        if (reason == null || reason.isEmpty())
          reasonStatusCode = "Unknown / invalid status code " + statusCode;
        else
          reasonStatusCode = "Unknown / invalid status code " + statusCode + ": " + reason;
      }
      try 
      {
    	  handlePageStatusCode(curURL, statusCode, reasonStatusCode);// Finds the status reason for all known statuses
      } 
      catch (Exception e)
      {
    	 logger.error("Unknown reason with statusCode: "+Integer.toString(statusCode));
    	 handlePageStatusCode(curURL, statusCode,"Unknown reason with statusCode: "+Integer.toString(statusCode));
      }
      Page page = new Page(curURL);
      page.setFetchResponseHeaders(fetchResult.getResponseHeaders());
      page.setStatusCode(statusCode);
      if (statusCode < 200 || statusCode > 299) { // Not 2XX: 2XX status codes indicate success
        if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY ||
            statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER ||
            statusCode == HttpStatus.SC_TEMPORARY_REDIRECT ||
            statusCode == 308) { // is 3xx  todo follow https://issues.apache.org/jira/browse/HTTPCORE-389

          page.setRedirect(true);
          if (myController.getConfig().isFollowRedirects()) {
            String movedToUrl = fetchResult.getMovedToUrl();
            if (movedToUrl == null) {
              throw new RedirectException(Level.WARN, "Unexpected error, URL: " + curURL + " is redirected to NOTHING");
            }
            page.setRedirectedToUrl(movedToUrl);
            
            WebURL webURL = new WebURL(movedToUrl);
            webURL.setParentDocid(curURL.getParentDocid());
            webURL.setParentUrl(curURL.getParentUrl());
            webURL.setSeedDocid(curURL.getSeedDocid());
            webURL.setDepth(curURL.getDepth());
            webURL.setAnchor(curURL.getAnchor());
            webURL.setPriority(curURL.getPriority());
            boolean isHttp = webURL.isHttp();
            if (isHttp && shouldVisit(page, webURL)) {
              if (robotstxtServer.allows(webURL)) {
                frontier.schedule(webURL);
              } else {
                logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
              }
            } else if (!isHttp) {
              logger.debug("Not visiting: {} - Protocol {} not supported", webURL.getURL(), webURL.getProtocol());
            } else {
              logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
            }
          }
        } else { // All other http codes other than 3xx & 200
          String description = "";
          description = EnglishReasonPhraseCatalog.INSTANCE
              .getReason(fetchResult.getStatusCode(), Locale.ENGLISH); // Finds the status reason for all known statuses
          String contentType = "";
          if (fetchResult.getEntity() != null && fetchResult.getEntity().getContentType() != null)
              contentType = fetchResult.getEntity().getContentType().getValue();
          onUnexpectedStatusCode(curURL.getURL(), fetchResult.getStatusCode(), contentType, description);
        }

      } else { // if status code is 200
        if (!curURL.getURL().equals(fetchResult.getFetchedUrl())) {
          if (docIdServer.isSeenBefore(fetchResult.getFetchedUrl())) {
            throw new RedirectException(Level.DEBUG, "Redirect page: " + curURL + " has already been seen");
          }
          curURL.setURL(fetchResult.getFetchedUrl());
          curURL.setDocid(docIdServer.getNewDocID(fetchResult.getFetchedUrl()));
        }

        if (!fetchResult.fetchContent(page, myController.getConfig().getMaxDownloadSize())) {
          throw new ContentFetchException();
        }
        
        if (page.isTruncated()) {
          logger.warn("Warning: unknown page size exceeded max-download-size, truncated to: ({}), at URL: {}",
              myController.getConfig().getMaxDownloadSize(), curURL.getURL());
        }

        parser.parse(page, curURL.getURL());
        
        // If the page is redirected by a <link rel="canonical"> tag or by
        // a <meta name="fragment" content="!"> tag to the _escaped_fragment_ version
        // of the page.
        if (page.isRedirect()) {
          WebURL webURL = new WebURL(page.redirectedToUrl);
          webURL.setParentDocid(curURL.getParentDocid());
          webURL.setParentUrl(curURL.getParentUrl());
          webURL.setSeedDocid(curURL.getSeedDocid());
          // Do increase the depth to avoid redirection loops
          webURL.setDepth((short)(curURL.getDepth() + 1));
          webURL.setAnchor(curURL.getAnchor());
          webURL.setPriority(curURL.getPriority());
          
          boolean isHttp = webURL.isHttp();
          if (isHttp && shouldVisit(page, webURL)) {
            if (robotstxtServer.allows(webURL)) {
              frontier.schedule(webURL);
            } else {
              logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
            }
          } else if (!isHttp) {
            logger.debug("Not visiting: {} - Protocol {} not supported", webURL.getURL(), webURL.getProtocol());
          } else {
            logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
          }
        }

        ParseData parseData = page.getParseData();
        List<WebURL> toSchedule = new ArrayList<>();
        int maxCrawlDepth = myController.getConfig().getMaxDepthOfCrawling();
        for (WebURL webURL : parseData.getOutgoingUrls()) {
          webURL.setParentDocid(curURL.getDocid());
          webURL.setParentUrl(curURL.getURL());
          webURL.setSeedDocid(curURL.getSeedDocid());
          webURL.setDepth((short) (curURL.getDepth() + 1));
          if ((maxCrawlDepth == -1) || (curURL.getDepth() < maxCrawlDepth)) {
            boolean isHttp = webURL.isHttp();
            if (isHttp && shouldVisit(page, webURL)) {
              if (robotstxtServer.allows(webURL)) {
                toSchedule.add(webURL);
              } else {
                logger.debug("Not visiting: {} as per the server's \"robots.txt\" policy", webURL.getURL());
              }
            } else if (!isHttp) {
              logger.debug("Not visiting: {} - Protocol {} not supported", webURL.getURL(), webURL.getProtocol());
            } else {
              logger.debug("Not visiting: {} as per your \"shouldVisit\" policy", webURL.getURL());
            }
          }
        }
        frontier.scheduleAll(toSchedule);

        visit(page);
      }
    } catch (PageBiggerThanMaxSizeException e) {
      onPageBiggerThanMaxSize(curURL.getURL(), e.getPageSize());
    } catch (ParseException pe) {
      onParseError(curURL);
    } catch (ContentFetchException cfe) {
      onContentFetchError(curURL);
    } catch (RedirectException re) {
      logger.log(re.level, re.getMessage());
    } catch (NotAllowedContentException nace) {
      logger.debug("Skipping: {} as it contains binary content which you configured not to crawl", curURL.getURL());
    } catch (Exception e) {
      onUnhandledException(curURL, e);
    } finally {
      if (fetchResult != null) {
        fetchResult.discardContentIfNotConsumed();
      }
    }
  }

  public Thread getThread() {
    return myThread;
  }
  
  public long getId() {
    return myThread.getId();
  }

  public void setThread(Thread myThread) {
    this.myThread = myThread;
  }

  public boolean isNotWaitingForNewURLs() {
    return !isWaitingForNewURLs;
  }
  
  @Override
  public String toString() {
    return "Crawler " + myId + " (Thread: " + myThread.getId() + ")";
  }
}
