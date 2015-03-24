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

public class CrawlConfig {

  /**
   * The folder which will be used by crawler for storing the intermediate
   * crawl data. The content of this folder should not be modified manually.
   */
  private String crawlStorageFolder;

  /**
   * If this feature is enabled, you would be able to resume a previously
   * stopped/crashed crawl. However, it makes crawling slightly slower
   */
  private boolean resumableCrawling = false;

  /**
   * Maximum depth of crawling For unlimited depth this parameter should be
   * set to -1
   */
  private int maxDepthOfCrawling = -1;

  /**
   * Maximum number of pages to fetch For unlimited number of pages, this
   * parameter should be set to -1
   */
  private int maxPagesToFetch = -1;

  /**
   * user-agent string that is used for representing your crawler to web
   * servers. See http://en.wikipedia.org/wiki/User_agent for more details
   */
  private String userAgentString = "crawler4j (http://code.google.com/p/crawler4j/)";

  /**
   * Politeness delay in milliseconds (delay between sending two requests to
   * the same host).
   */
  private int politenessDelay = 200;

  /**
   * Should we also crawl https pages?
   */
  private boolean includeHttpsPages = true;

  /**
   * Should we fetch binary content such as images, audio, ...?
   */
  private boolean includeBinaryContentInCrawling = false;
  
  /**
   * Should we process binary content such as image, audio, ... using TIKA?
   */
  private boolean processBinaryContentInCrawling = false;

  /**
   * Maximum Connections per host
   */
  private int maxConnectionsPerHost = 100;

  /**
   * Maximum total connections
   */
  private int maxTotalConnections = 100;

  /**
   * Socket timeout in milliseconds
   */
  private int socketTimeout = 20000;

  /**
   * Connection timeout in milliseconds
   */
  private int connectionTimeout = 30000;

  /**
   * Max number of outgoing links which are processed from a page
   */
  private int maxOutgoingLinksToFollow = 5000;

  /**
   * Max allowed size of a page. Pages larger than this size will not be
   * fetched.
   */
  private int maxDownloadSize = 1048576;

  /**
   * Should we follow redirects?
   */
  private boolean followRedirects = true;
  
  /**
   * Should the TLD list be updated automatically on each run? Alternatively,
   * it can be loaded from the embedded tld-names.zip file that was obtained from 
   * https://publicsuffix.org/list/effective_tld_names.dat
   */
  private boolean onlineTldListUpdate = false;
  
  /**
   * Should the robots.txt directives be ignored when adding a seed? If this is set to true,
   * a seed will be fetched even if robots.txt disallows it.
   */
  private boolean ignoreRobotsTxtForSeed = false;

  /**
   * Should the crawler stop running when the queue is empty?
   */
  private boolean shutdownOnEmptyQueue = true;
  
  /**
   * If crawler should run behind a proxy, this parameter can be used for
   * specifying the proxy host.
   */
  private String proxyHost = null;

  /**
   * If crawler should run behind a proxy, this parameter can be used for
   * specifying the proxy port.
   */
  private int proxyPort = 80;

  /**
   * If crawler should run behind a proxy and user/pass is needed for
   * authentication in proxy, this parameter can be used for specifying the
   * username.
   */
  private String proxyUsername = null;

  /**
   * If crawler should run behind a proxy and user/pass is needed for
   * authentication in proxy, this parameter can be used for specifying the
   * password.
   */
  private String proxyPassword = null;

  public CrawlConfig() {
  }

  /**
   * Validates the configs specified by this instance.
   *
   * @throws Exception Whenever the configuration is not correct.
   */
  public void validate() throws Exception {
    if (crawlStorageFolder == null) {
      throw new Exception("Crawl storage folder is not set in the CrawlConfig.");
    }
    if (politenessDelay < 0) {
      throw new Exception("Invalid value for politeness delay: " + politenessDelay);
    }
    if (maxDepthOfCrawling < -1) {
      throw new Exception("Maximum crawl depth should be either a positive number or -1 for unlimited depth.");
    }
    if (maxDepthOfCrawling > Short.MAX_VALUE) {
      throw new Exception("Maximum value for crawl depth is " + Short.MAX_VALUE);
    }
  }

  public String getCrawlStorageFolder() {
    return crawlStorageFolder;
  }

  /**
   * The folder which will be used by crawler for storing the intermediate
   * crawl data. The content of this folder should not be modified manually.
   * 
   * @param crawlStorageFolder The storage folder where to store crawl data
   */
  public void setCrawlStorageFolder(String crawlStorageFolder) {
    this.crawlStorageFolder = crawlStorageFolder;
  }

  public boolean isResumableCrawling() {
    return resumableCrawling;
  }

  /**
   * If this feature is enabled, you would be able to resume a previously
   * stopped/crashed crawl. However, it makes crawling slightly slower
   * 
   * @param resumableCrawling True to enable resumable crawling, false to disable
   */
  public void setResumableCrawling(boolean resumableCrawling) {
    this.resumableCrawling = resumableCrawling;
  }

  public int getMaxDepthOfCrawling() {
    return maxDepthOfCrawling;
  }

  /**
   * Maximum depth of crawling For unlimited depth this parameter should be
   * set to -1
   * 
   * @param maxDepthOfCrawling The maximum depth of crawling.
   */
  public void setMaxDepthOfCrawling(int maxDepthOfCrawling) {
    this.maxDepthOfCrawling = maxDepthOfCrawling;
  }

  public int getMaxPagesToFetch() {
    return maxPagesToFetch;
  }

  /**
   * Maximum number of pages to fetch For unlimited number of pages, this
   * parameter should be set to -1
   * 
   * @param maxPagesToFetch The maximum number of pages to fetch.
   */
  public void setMaxPagesToFetch(int maxPagesToFetch) {
    this.maxPagesToFetch = maxPagesToFetch;
  }

  public String getUserAgentString() {
    return userAgentString;
  }

  /**
   * user-agent string that is used for representing your crawler to web
   * servers. See http://en.wikipedia.org/wiki/User_agent for more details
   * 
   * @param userAgentString The user-agent used during crawling
   */
  public void setUserAgentString(String userAgentString) {
    this.userAgentString = userAgentString;
  }

  public int getPolitenessDelay() {
    return politenessDelay;
  }

  /**
   * Politeness delay in milliseconds (delay between sending two requests to
   * the same host).
   *
   * @param politenessDelay
   *            the delay in milliseconds.
   */
  public void setPolitenessDelay(int politenessDelay) {
    this.politenessDelay = politenessDelay;
  }

  public boolean isIncludeHttpsPages() {
    return includeHttpsPages;
  }

  /**
   * Should we also crawl https pages?
   * 
   * @param includeHttpsPages True to crawl https pages, false to ignore them.
   */
  public void setIncludeHttpsPages(boolean includeHttpsPages) {
    this.includeHttpsPages = includeHttpsPages;
  }

  public boolean isIncludeBinaryContentInCrawling() {
    return includeBinaryContentInCrawling;
  }

  /**
   * Should we fetch binary content such as images, audio, ...?
   * 
   * @param includeBinaryContentInCrawling True to crawl binary content, false to ignore it.
   */
  public void setIncludeBinaryContentInCrawling(boolean includeBinaryContentInCrawling) {
    this.includeBinaryContentInCrawling = includeBinaryContentInCrawling;
  }
  
  public boolean isProcessBinaryContentInCrawling() {
    return processBinaryContentInCrawling;
  }

  /**
   * Should we process binary content such as images, audio, ... using TIKA?
   * 
   * @param processBinaryContentInCrawling True to process binary content using TIKA in order
   * 									   to extract more links, false to pass it on as-is
   */
  public void setProcessBinaryContentInCrawling(boolean processBinaryContentInCrawling) {
    this.processBinaryContentInCrawling = processBinaryContentInCrawling;
  }  

  public int getMaxConnectionsPerHost() {
    return maxConnectionsPerHost;
  }

  /**
   * Maximum Connections per host
   * 
   * @param maxConnectionsPerHost The maximum number of simultaneous connections per host
   */
  public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
    this.maxConnectionsPerHost = maxConnectionsPerHost;
  }

  public int getMaxTotalConnections() {
    return maxTotalConnections;
  }

  /**
   * Maximum total connections
   * 
   * @param maxTotalConnections The maxmimum number of total connections
   */
  public void setMaxTotalConnections(int maxTotalConnections) {
    this.maxTotalConnections = maxTotalConnections;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * Socket timeout in milliseconds
   * 
   * @param socketTimeout The socket timeout
   */
  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  /**
   * Connection timeout in milliseconds
   * 
   * @param connectionTimeout The connection timeout
   */
  public void setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public int getMaxOutgoingLinksToFollow() {
    return maxOutgoingLinksToFollow;
  }

  /**
   * Max number of outgoing links which are processed from a page
   * 
   * @param maxOutgoingLinksToFollow The maximum number of outgoing links to follow
   */
  public void setMaxOutgoingLinksToFollow(int maxOutgoingLinksToFollow) {
    this.maxOutgoingLinksToFollow = maxOutgoingLinksToFollow;
  }

  public int getMaxDownloadSize() {
    return maxDownloadSize;
  }

  /**
   * Max allowed size of a page. Pages larger than this size will not be
   * fetched.
   * 
   * @param maxDownloadSize The maximum download size
   */
  public void setMaxDownloadSize(int maxDownloadSize) {
    this.maxDownloadSize = maxDownloadSize;
  }

  public boolean isFollowRedirects() {
    return followRedirects;
  }

  /**
   * Should we follow redirects?
   * 
   * @param followRedirects True to follow redirects, false to skip them.
   */
  public void setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
  }
  
  public boolean isOnlineTldListUpdate() {
      return onlineTldListUpdate;
  }
  
  /**
   * Should the TLD list be updated automatically on each run? Alternatively,
   * it can be loaded from the embedded tld-names.zip file that was obtained from 
   * https://publicsuffix.org/list/effective_tld_names.dat
   * 
   * @param online Whether to load the list online
   */
  public void setOnlineTldListUpdate(boolean online) {
      onlineTldListUpdate = online;
  }
  
  public boolean isIgnoreRobotsTxtForSeed() {
      return ignoreRobotsTxtForSeed;
  }
  
  /**
   * Should the robots.txt directives be ignored when adding a seed? If this is set to true,
   * a seed will be fetched even if robots.txt disallows it.
   * 
   * @param ignore True to ignore robots.txt directives for seed URLs, false to obey robots.txt always
   */
  public void setIgnoreRobotsTxtForSeed(boolean ignore) {
      ignoreRobotsTxtForSeed = ignore;
  }
  
  public boolean isShutdownOnEmptyQueue() {
      return shutdownOnEmptyQueue;
  }
  
  /**
   * Should the crawler stop running when the queue is empty?
   * 
   * @param shutdown True to shut down on empty queue, false to keep waiting
   */
  public void setShutdownOnEmptyQueue(boolean shutdown) {
      shutdownOnEmptyQueue = shutdown;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * If crawler should run behind a proxy, this parameter can be used for
   * specifying the proxy host.
   * 
   * @param proxyHost The hostname of the proxy server
   */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public int getProxyPort() {
    return proxyPort;
  }

  /**
   * If crawler should run behind a proxy, this parameter can be used for
   * specifying the proxy port.
   * 
   * @param proxyPort The port of the proxy server
   */
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * If crawler should run behind a proxy and user/pass is needed for
   * authentication in proxy, this parameter can be used for specifying the
   * username.
   * 
   * @param proxyUsername The username to authenticate to the proxy server
   */
  public void setProxyUsername(String proxyUsername) {
    this.proxyUsername = proxyUsername;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * If crawler should run behind a proxy and user/pass is needed for
   * authentication in proxy, this parameter can be used for specifying the
   * password.
   * 
   * @param proxyPassword The password to authenticate to the proxy serer
   */
  public void setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Crawl storage folder: " + getCrawlStorageFolder() + "\n");
    sb.append("Resumable crawling: " + isResumableCrawling() + "\n");
    sb.append("Max depth of crawl: " + getMaxDepthOfCrawling() + "\n");
    sb.append("Max pages to fetch: " + getMaxPagesToFetch() + "\n");
    sb.append("User agent string: " + getUserAgentString() + "\n");
    sb.append("Include https pages: " + isIncludeHttpsPages() + "\n");
    sb.append("Include binary content: " + isIncludeBinaryContentInCrawling() + "\n");
    sb.append("Max connections per host: " + getMaxConnectionsPerHost() + "\n");
    sb.append("Max total connections: " + getMaxTotalConnections() + "\n");
    sb.append("Socket timeout: " + getSocketTimeout() + "\n");
    sb.append("Max total connections: " + getMaxTotalConnections() + "\n");
    sb.append("Max outgoing links to follow: " + getMaxOutgoingLinksToFollow() + "\n");
    sb.append("Max download size: " + getMaxDownloadSize() + "\n");
    sb.append("Should follow redirects?: " + isFollowRedirects() + "\n");
    sb.append("Proxy host: " + getProxyHost() + "\n");
    sb.append("Proxy port: " + getProxyPort() + "\n");
    sb.append("Proxy username: " + getProxyUsername() + "\n");
    sb.append("Proxy password: " + getProxyPassword() + "\n");
    return sb.toString();
  }
}