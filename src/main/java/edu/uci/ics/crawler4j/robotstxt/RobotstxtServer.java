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

package edu.uci.ics.crawler4j.robotstxt;

import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.exceptions.PageBiggerThanMaxSizeException;
import edu.uci.ics.crawler4j.fetcher.PageFetchResult;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */
public class RobotstxtServer {

  private static final Logger logger = LoggerFactory.getLogger(RobotstxtServer.class);

  protected RobotstxtConfig config;

  protected final Map<String, HostDirectives> host2directivesCache = new HashMap<>();

  protected PageFetcher pageFetcher;

  public RobotstxtServer(RobotstxtConfig config, PageFetcher pageFetcher) {
    this.config = config;
    this.pageFetcher = pageFetcher;
  }

  private static String getHost(URL url) {
    return url.getHost().toLowerCase();
  }

  /** Please note that in the case of a bad URL, TRUE will be returned */
  public boolean allows(WebURL webURL) {
    if (!config.isEnabled()) {
      return true;
    }
    try {
      URL url = new URL(webURL.getURL());
      String host = getHost(url);
      String path = url.getPath();

      HostDirectives directives = host2directivesCache.get(host);

      if (directives != null && directives.needsRefetch()) {
        synchronized (host2directivesCache) {
          host2directivesCache.remove(host);
          directives = null;
        }
      }
      
      if (directives == null) {
        directives = fetchDirectives(url);
      }
      return directives.allows(path);
    } catch (MalformedURLException e) {
      logger.error("Bad URL in Robots.txt: " + webURL.getURL(), e);
    }

    logger.warn("RobotstxtServer: default: allow", webURL.getURL());
    return true;
  }
  
  /**
   * Get the cached robots.txt directives for a URL. This will never
   * fetch the robots.txt file, but only return the cached directives
   * if they are available.
   * 
   * @param url The URL for which to get the directives
   * @return HostDirectives object for the host, or null if no directives are available
   */
  public HostDirectives getDirectives(WebURL weburl) {
    String host;
    try {
      URL url = new URL(weburl.getURL());
      host = getHost(url);
    } catch (MalformedURLException e) {
      return null;
    }
    
    synchronized (host2directivesCache) {
      return host2directivesCache.get(host);
    }
  }

  private HostDirectives fetchDirectives(URL url) {
    WebURL robotsTxtUrl = new WebURL();
    String host = getHost(url);
    String port = ((url.getPort() == url.getDefaultPort()) || (url.getPort() == -1)) ? "" : (":" + url.getPort());
    String proto = url.getProtocol();
    robotsTxtUrl.setURL(proto + "://" + host + port + "/robots.txt");
    HostDirectives directives = null;
    PageFetchResult fetchResult = null;
    try {
      for (int redir = 0; redir < 3; ++redir) {
        try {
          fetchResult = pageFetcher.fetchPage(robotsTxtUrl);
        } catch (javax.net.ssl.SSLHandshakeException e) {
          logger.info("SSL Exception while requesting robots.txt from {}", url.toString());
          break;
        }
        int status = fetchResult.getStatusCode();
        // Follow redirects up to 3 levels
        if ((status == HttpStatus.SC_MULTIPLE_CHOICES ||
            status == HttpStatus.SC_MOVED_PERMANENTLY || 
            status == HttpStatus.SC_MOVED_TEMPORARILY || 
            status == HttpStatus.SC_SEE_OTHER || 
            status == HttpStatus.SC_TEMPORARY_REDIRECT || 
            status == 308) && // SC_PERMANENT_REDIRECT RFC7538
            fetchResult.getMovedToUrl() != null) {
            
          fetchResult.discardContentIfNotConsumed();
          String new_url = fetchResult.getMovedToUrl();
          if (new_url.endsWith("robots.txt")) {
            robotsTxtUrl.setURL(new_url);
          } else {
            logger.info("While fetching robots.txt from {}, redirected to non-robots.txt URL: {}. Assuming no robots.txt for this domain", fetchResult.getFetchedUrl(), new_url);
            break;
          }
        }
        else // Done on all other occasions
          break;
      }
      
      if (fetchResult != null && fetchResult.getStatusCode() == HttpStatus.SC_OK) {
        Page page = new Page(robotsTxtUrl);
        fetchResult.fetchContent(page, 16384);
        if (Util.hasPlainTextContent(page.getContentType())) {
          try {
            String content = "";
            if (page.getContentData() != null) {
              if (page.getContentCharset() == null && page.getContentData() != null) {
                content = new String(page.getContentData());
              } else {
                content = new String(page.getContentData(), page.getContentCharset());
              }
              directives = RobotstxtParser.parse(robotsTxtUrl.getURL(), content, config);
            } else {
              logger.info("No data received for robots.txt retrieved from URL: {}", robotsTxtUrl.getURL());
            }
          } catch (Exception e) {
            logger.error("Error occurred while fetching (robots) url: " + robotsTxtUrl.getURL(), e);
          }
        } else if (page.getContentType().contains("html")) { // TODO This one should be upgraded to remove all html tags
          String content = new String(page.getContentData());
          directives = RobotstxtParser.parse(robotsTxtUrl.getURL(), content, config);
        } else {
          logger.warn("Can't read this robots.txt: {}  as it is not written in plain text, contentType: {}",
                      robotsTxtUrl.getURL(), page.getContentType());
        }
      } else if (fetchResult != null) {
        logger.debug("Can't read this robots.txt: {}  as it's status code is {}", robotsTxtUrl.getURL(),
                     fetchResult.getStatusCode());
      }
    } catch (SocketException | UnknownHostException | SocketTimeoutException | NoHttpResponseException se) {
      // No logging here, as it just means that robots.txt doesn't exist on this server which is perfectly ok
    } catch (PageBiggerThanMaxSizeException pbtms) {
      logger.error("Error occurred while fetching (robots) url: {}, {}", robotsTxtUrl.getURL(), pbtms.getMessage());
    } catch (Exception e) {
      logger.error("Error occurred while fetching (robots) url: " + robotsTxtUrl.getURL(), e);
    } finally {
      if (fetchResult != null) {
        fetchResult.discardContentIfNotConsumed();
      }
    }

    if (directives == null) {
      // We still need to have this object to keep track of the time we
      // fetched it
      directives = new HostDirectives(config);
    }
    synchronized (host2directivesCache) {
      if (host2directivesCache.size() == config.getCacheSize()) {
        String minHost = null;
        long minAccessTime = Long.MAX_VALUE;
        for (Entry<String, HostDirectives> entry : host2directivesCache.entrySet()) {
          long entryAccessTime = entry.getValue().getLastAccessTime();
          if (entryAccessTime < minAccessTime) {
            minAccessTime = entryAccessTime;
            minHost = entry.getKey();
          }
        }
        host2directivesCache.remove(minHost);
      }
      host2directivesCache.put(host, directives);
    }
    return directives;
  }
}
