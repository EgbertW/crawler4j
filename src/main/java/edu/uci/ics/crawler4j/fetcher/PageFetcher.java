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

package edu.uci.ics.crawler4j.fetcher;

import java.io.IOException;
import java.io.InputStream;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar <lastname at gmail dot com>
 */
public class PageFetcher extends Configurable {

  protected static final Logger logger = LoggerFactory.getLogger(PageFetcher.class);

  protected PoolingHttpClientConnectionManager connectionManager;

  protected CloseableHttpClient httpClient;

  protected final Object mutex = new Object();

  protected long lastFetchTime = 0;
  
  protected Long fetchErrorCounter = 0l;

  protected IdleConnectionMonitorThread connectionMonitorThread = null;

  public PageFetcher(CrawlConfig config) {
    super(config);

    RequestConfig requestConfig = RequestConfig.custom()
        .setExpectContinueEnabled(false)
        .setCookieSpec(CookieSpecs.BROWSER_COMPATIBILITY)
        .setRedirectsEnabled(false)
        .setSocketTimeout(config.getSocketTimeout())
        .setConnectTimeout(config.getConnectionTimeout())
        .build();

    RegistryBuilder<ConnectionSocketFactory> connRegistryBuilder = RegistryBuilder.create();
    connRegistryBuilder.register("http", PlainConnectionSocketFactory.INSTANCE);
    if (config.isIncludeHttpsPages()) {
      try { // Fixing: https://code.google.com/p/crawler4j/issues/detail?id=174
        // By always trusting the ssl certificate
        SSLContext sslContext = SSLContexts.custom()
            .loadTrustMaterial(null, new TrustStrategy() {
              @Override
              public boolean isTrusted(final X509Certificate[] chain, String authType) {
                return true;
              }
            }).build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
            sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        connRegistryBuilder.register("https", sslsf);
      } catch (Exception e) {
        logger.debug("Exception thrown while trying to register https:", e);
      }
    }

    Registry<ConnectionSocketFactory> connRegistry = connRegistryBuilder.build();
    connectionManager = new PoolingHttpClientConnectionManager(connRegistry);
    connectionManager.setMaxTotal(config.getMaxTotalConnections());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnectionsPerHost());

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    clientBuilder.setDefaultRequestConfig(requestConfig);
    clientBuilder.setConnectionManager(connectionManager);
    clientBuilder.setUserAgent(config.getUserAgentString());
    if (config.getProxyHost() != null) {

      if (config.getProxyUsername() != null) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            new AuthScope(config.getProxyHost(), config.getProxyPort()),
            new UsernamePasswordCredentials(config.getProxyUsername(), config.getProxyPassword()));
        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }

      HttpHost proxy = new HttpHost(config.getProxyHost(), config.getProxyPort());
      clientBuilder.setProxy(proxy);
    }
    clientBuilder.addInterceptorLast(new HttpResponseInterceptor() {
      @Override
      public void process(final HttpResponse response, final HttpContext context) throws HttpException, IOException {
        HttpEntity entity = response.getEntity();
        Header contentEncoding = entity.getContentEncoding();
        if (contentEncoding != null) {
          HeaderElement[] codecs = contentEncoding.getElements();
          for (HeaderElement codec : codecs) {
            if (codec.getName().equalsIgnoreCase("gzip")) {
              response.setEntity(new GzipDecompressingEntity(response.getEntity()));
              return;
            }
          }
        }
      }
    });

    httpClient = clientBuilder.build();

    if (connectionMonitorThread == null) {
      connectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
    }
    connectionMonitorThread.start();
  }

  public PageFetchResult fetchHeader(WebURL webUrl) {
    PageFetchResult fetchResult = new PageFetchResult();
    String toFetchURL = webUrl.getURL();
    HttpGet get = null;
    try {
      get = new HttpGet(toFetchURL);
      synchronized (mutex) {
        long now = (new Date()).getTime();
        if (now - lastFetchTime < config.getPolitenessDelay()) {
          Thread.sleep(config.getPolitenessDelay() - (now - lastFetchTime));
        }
        lastFetchTime = (new Date()).getTime();
      }

      HttpResponse response = httpClient.execute(get);
      fetchResult.setEntity(response.getEntity());
      fetchResult.setResponseHeaders(response.getAllHeaders());

      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        if (statusCode != HttpStatus.SC_NOT_FOUND) {
          if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY
              || statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER
              || statusCode == HttpStatus.SC_TEMPORARY_REDIRECT || statusCode == CustomFetchStatus.SC_PERMANENT_REDIRECT) {
            Header header = response.getFirstHeader("Location");
            if (header != null) {
              String movedToUrl = header.getValue();
              movedToUrl = URLCanonicalizer.getCanonicalURL(movedToUrl, toFetchURL);
              fetchResult.setMovedToUrl(movedToUrl);
            }
            fetchResult.setStatusCode(statusCode);
            return fetchResult;
          }
          logger.info("Failed: {}, while fetching {}", response.getStatusLine().toString(), toFetchURL);
        }
        fetchResult.setStatusCode(response.getStatusLine().getStatusCode());
        return fetchResult;
      }

      fetchResult.setFetchedUrl(toFetchURL);
      String uri = get.getURI().toString();
      if (!uri.equals(toFetchURL)) {
        if (!URLCanonicalizer.getCanonicalURL(uri).equals(toFetchURL)) {
          fetchResult.setFetchedUrl(uri);
        }
      }

      if (fetchResult.getEntity() != null) {
        long size = fetchResult.getEntity().getContentLength();
        if (size == -1) {
          Header length = response.getLastHeader("Content-Length");
          if (length == null) {
            length = response.getLastHeader("Content-length");
          }
          if (length != null) {
            size = Integer.parseInt(length.getValue());
          } else {
            size = -1;
          }
        }
        if (size > config.getMaxDownloadSize()) {
          fetchResult.setStatusCode(CustomFetchStatus.PageTooBig);
          get.abort();
          logger.warn("Failed: Page Size ({}) exceeded max-download-size ({}), at URL: {}",
              size, config.getMaxDownloadSize(), webUrl.getURL());
          return fetchResult;
        }

        fetchResult.setStatusCode(HttpStatus.SC_OK);
        return fetchResult;
      }

      get.abort();
    } catch (Exception e) {
      handleException(webUrl, e, fetchResult);
      return fetchResult;
    } finally {
      try {
        if (fetchResult.getEntity() == null && get != null) {
          get.abort();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // If the request was not handled, something went wrong. Report unknown error.
    fetchResult.setStatusCode(CustomFetchStatus.UnknownError);
    logger.error("Failed: Unknown error occurred while fetching {}", webUrl.getURL());
    return fetchResult;
  }
  
  /**
   * Handle any exceptions that occur during the fetching of the page. 
   * 
   * @param webUrl The URL that it attempted to retrieve
   * @param e The exception that was thrown
   * @param fetchResult The fetch result, may be changed to give status information
   */
  private void handleException(WebURL webUrl, Exception e, PageFetchResult fetchResult) {
    // Get a unique ID for this fetch error to keep the overview of multiple 
    // messages in case of a synchronous logging over multiple threads
    long cur_error;
    synchronized (fetchErrorCounter) {
        cur_error = ++fetchErrorCounter;
    }
    
    // The message that should be logged
    Object msg = e.getMessage() != null && !e.getMessage().isEmpty() ? e.getMessage() : e.getCause();
    
    // First parse the URL into a URI object
    URI url;
    try {
      url = new URI(webUrl.getURL());
    } catch (URISyntaxException uri_ex) {
      logger.error("[Fetch error #{}] Invalid URL {} caused an exception: {}", cur_error, webUrl.getURL(), msg);
      logger.debug("[Fetch error #{}] Stacktrace: ", cur_error, e);
      return;
    }
    
    if (url.toString().toLowerCase().endsWith("robots.txt")) {
      // Ignoring this Exception as it just means that we tried to parse a robots.txt file which this site doesn't have
      // Which is ok, so no exception should be thrown
      logger.error("[Fetch error #{}] Could not retrieve robots.txt from {}: {}", cur_error, url, msg);
      return;
    }

    boolean log = true;
    boolean log_as_error = false;
    
    logger.error("[Fetch error #{}] An exception occurred while fetching {}: {}", cur_error, webUrl.getURL(), e.getMessage() != null ? e.getMessage() : e.getCause());
    logger.debug("[Fetch error #{}] Exception class: {}", cur_error, e.getClass().getName());
    
    if (e instanceof NullPointerException) {
      logger.error("[Fetch error #{}] Empty page", cur_error, msg);
      fetchResult.setStatusCode(CustomFetchStatus.PageEmpty);
    } else if (e instanceof NoRouteToHostException) {
      logger.error("[Fetch error #{}] No route to hostname {}. Error: {}", cur_error, url.getHost(), msg);
    } else if (e instanceof HttpHostConnectException) {
      logger.error("[Fetch errro #{}] Could not establish connection to host {}. Error: {}", cur_error, url.getHost(), msg);
      fetchResult.setStatusCode(CustomFetchStatus.ConnectionRefused);
    } else if (e instanceof UnknownHostException) {
      logger.error("[Fetch error #{}] Unknown host {}. Error: {}", cur_error, url.getHost(), msg);
      fetchResult.setStatusCode(CustomFetchStatus.UnknownHostError);
    } else if (e instanceof SocketTimeoutException || e instanceof ConnectTimeoutException || e instanceof SocketException) {
      logger.error("[Fetch error #{}] Timeout while fetching page: {}", cur_error, msg);
      fetchResult.setStatusCode(CustomFetchStatus.SocketTimeoutError);
    } else if (e instanceof IOException) {
      logger.error("[Fetch error #{}] Fatal transport error: {})", cur_error, msg);
      fetchResult.setStatusCode(CustomFetchStatus.FatalTransportError);
    } else if (e instanceof IllegalStateException) {
      logger.error("[Fetch error #{}] IllegalStateException: {}", cur_error, msg);
      // ignoring exceptions that occur because of not registering https
      // and other schemes
    } else {
      // Escalate log level for unexpected errors to 
      log_as_error = true;
      logger.error("[Fetch error #{}] Unexpected error: {}", cur_error, msg);
    }
    
    // Log diagnostic information about the exception and the URL 
    if (log_as_error)
    {
      logger.error("[Fetch error #{}] Diagnostic information", cur_error);
      logger.error("[Fetch error #{}] Seed document ID: {}", cur_error, webUrl.getSeedDocid());
      if (webUrl.getParentUrl() != null && !webUrl.getParentUrl().isEmpty()) {
          logger.error("[Fetch error #{}] Parent doc id: {}", cur_error, webUrl.getParentDocid());
          logger.error("[Fetch error #{}] Parent URL: {}", cur_error, webUrl.getParentUrl());
      }
      logger.error("[Fetch error #{}] Crawl depth: {}", cur_error, webUrl.getDepth());
      logger.error("[Fetch error #{}] Stacktrace:", e);
    } else if (log) {
      logger.debug("[Fetch error #{}] Diagnostic information", cur_error);
      logger.debug("[Fetch error #{}] Seed document ID: {}", cur_error, webUrl.getSeedDocid());
      if (webUrl.getParentUrl() != null && !webUrl.getParentUrl().isEmpty()) {
        logger.debug("[Fetch error #{}] Parent doc id: {}", cur_error, webUrl.getParentDocid());
        logger.debug("[Fetch error #{}] Parent URL: {}", cur_error, webUrl.getParentUrl());
      }
      logger.debug("[Fetch error #{}] Crawl depth: {}", cur_error, webUrl.getDepth());
      if (logger.isTraceEnabled())
        logger.trace("[Fetch error #" + cur_error + "] Stacktrace:", e);
    }
  }
  
  public synchronized void shutDown() {
    if (connectionMonitorThread != null) {
      connectionManager.shutdown();
      connectionMonitorThread.shutdown();
    }
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }


  private static class GzipDecompressingEntity extends HttpEntityWrapper {

    public GzipDecompressingEntity(final HttpEntity entity) {
      super(entity);
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {

      // the wrapped entity's getContent() decides about repeatability
      InputStream wrappedin = wrappedEntity.getContent();

      return new GZIPInputStream(wrappedin);
    }

    @Override
    public long getContentLength() {
      // length of ungzipped content is not known
      return -1;
    }
  }
}