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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLContext;

import edu.uci.ics.crawler4j.crawler.authentication.NtAuthInfo;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.Configurable;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.authentication.AuthInfo;
import edu.uci.ics.crawler4j.crawler.authentication.BasicAuthInfo;
import edu.uci.ics.crawler4j.crawler.authentication.FormAuthInfo;
import edu.uci.ics.crawler4j.crawler.exceptions.PageBiggerThanMaxSizeException;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar
 */
public class PageFetcher extends Configurable {
  protected static final Logger logger = LoggerFactory.getLogger(PageFetcher.class);

  private static class HostRequests {
    /** The number of outstanding requests */
    int outstanding = 0;
    /** The next time a request to this host may be made */
    long nextFetchTime = System.currentTimeMillis();
    /** The politeness delay used for this host */
    long delay = 200;
    /** 
     * The penalty to take into account while selecting the best URL to fetch.
     * This value is excluded from the actual waiting time and is increased
     * when it has been selected using getBestURL.
     */
    long penalty = 0;
  }
  
  protected PoolingHttpClientConnectionManager connectionManager;
  protected CloseableHttpClient httpClient;
  protected final Object mutex = new Object();
  protected Map<String, HostRequests> nextFetchTimes;
  protected IdleConnectionMonitorThread connectionMonitorThread = null;
  
  protected long delay_total = 0;
  protected int delay_counter = 0;
  protected long delay_last_time = 0;
  
  public PageFetcher(CrawlConfig config) {
    super(config);

    RequestConfig requestConfig =
        RequestConfig.custom().setExpectContinueEnabled(false).setCookieSpec(CookieSpecs.DEFAULT)
                     .setRedirectsEnabled(false).setSocketTimeout(config.getSocketTimeout())
                     .setConnectTimeout(config.getConnectionTimeout()).build();

    RegistryBuilder<ConnectionSocketFactory> connRegistryBuilder = RegistryBuilder.create();
    connRegistryBuilder.register("http", PlainConnectionSocketFactory.INSTANCE);    if (config.isIncludeHttpsPages()) {
      try { // Fixing: https://code.google.com/p/crawler4j/issues/detail?id=174
        // By always trusting the ssl certificate
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
          @Override
          public boolean isTrusted(final X509Certificate[] chain, String authType) {
            return true;
          }
        }).build();
        SSLConnectionSocketFactory sslsf =
            new SniSSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
        connRegistryBuilder.register("https", sslsf);
      } catch (Exception e) {
        logger.warn("Exception thrown while trying to register https");
        logger.debug("Stacktrace", e);
      }
    }

    Registry<ConnectionSocketFactory> connRegistry = connRegistryBuilder.build();
    connectionManager = new SniPoolingHttpClientConnectionManager(connRegistry);
    connectionManager.setMaxTotal(config.getMaxTotalConnections());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnectionsPerHost());

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    clientBuilder.setDefaultRequestConfig(requestConfig);
    clientBuilder.setConnectionManager(connectionManager);
    clientBuilder.setUserAgent(config.getUserAgentString());
    clientBuilder.setDefaultHeaders(config.getDefaultHeaders());

    if (config.getProxyHost() != null) {
      if (config.getProxyUsername() != null) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(new AuthScope(config.getProxyHost(), config.getProxyPort()),
                                           new UsernamePasswordCredentials(config.getProxyUsername(),
                                                                           config.getProxyPassword()));
        clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }

      HttpHost proxy = new HttpHost(config.getProxyHost(), config.getProxyPort());
      clientBuilder.setProxy(proxy);
      logger.debug("Working through Proxy: {}", proxy.getHostName());
    }

    nextFetchTimes = new HashMap<String, HostRequests>();
    
    httpClient = clientBuilder.build();
    if ((config.getAuthInfos() != null) && !config.getAuthInfos().isEmpty()) {
      doAuthetication(config.getAuthInfos());
    }

    if (connectionMonitorThread == null) {
      connectionMonitorThread = new IdleConnectionMonitorThread(connectionManager);
    }
    connectionMonitorThread.start();
  }
  
  /**
   * Set the politeness delay for a specific host.
   * 
   * @param host The host for which to set a delay
   * @param timeout The timeout to set for this host
   */
  public void setPolitenessDelay(String host, long timeout) {
    synchronized (nextFetchTimes) {
      HostRequests req = nextFetchTimes.get(host);
      if (req == null)
        nextFetchTimes.put(host, req = new HostRequests());
      req.nextFetchTime += timeout;
      req.delay = config.getPolitenessDelay();
    }
  }
  
  public long getPolitenessDelay(String host) {
    synchronized (nextFetchTimes) {
      HostRequests req = nextFetchTimes.get(host);
      if (req == null)
        return config.getPolitenessDelay();
      return req.delay;
    }
  }
  
  public long getDefaultPolitenessDelay() {
    return config.getPolitenessDelay();
  }
  
  /**
   * Find the URL with the lowest nextFetchTime, to optimally utilize the
   * available threads while respecting the politeness delay.
   * 
   * @param urls A collection of URLs from which one will be selected
   * @param max The maximum number of milliseconds that the URL should be in
   *            the future. If no URL matches this criterion, null will be returned
   * @return The best URL, or null if no URL has a politeness delay shorter than max
   */
  public WebURL getBestURL(Collection<WebURL> urls, long max) {
    // Return null if there's nothing to choose from
    if (urls.size() == 0)
      return null;
      
    long now = System.currentTimeMillis();
    Long min_delay = null;
    WebURL min_url = null;
    HostRequests best_req = null;
    synchronized (nextFetchTimes) {
      for (WebURL webUrl : urls) {
        try {
          URI url = new URI(webUrl.getURL());
          String host = url.getHost();
          HostRequests target_time = nextFetchTimes.get(host);
          if (target_time == null) {
            // Currently, no fetch time is available. This makes a good
            // candidate. Do add a new entry for HostRequests, because
            // the penalty needs to be stored to avoid selecting it again.
            target_time = new HostRequests();
            target_time.nextFetchTime = now;
            target_time.penalty = target_time.delay;
            nextFetchTimes.put(host, target_time);
            return webUrl;
          }
          
          long delay = target_time.nextFetchTime + target_time.penalty - now;
          
          // A negative time or 0 time is instant crawl
          if (delay <= 0) {
            target_time.penalty += target_time.delay;
            return webUrl;
          }
          
          if (min_delay == null || delay < min_delay) {
            min_delay = delay;
            min_url = webUrl;
            best_req = target_time;
          }
        }
        catch (URISyntaxException e) {
          // Invalid URL, will not succeed, might as well get over with it
          return webUrl;
        }
      }
      
      // Do not return any URL when the max is exceeded
      if (min_delay != null && min_delay > max)
          return null;
      
      // Add the politeness delay to this host already as it's probably going to
      // be crawled which makes it less attractive for a following request to also
      // crawl from the same host
      if (best_req != null)
          best_req.penalty += best_req.delay;
    }
    
    // There should be a 'best' URL always at this point
    assert(min_url != null);
    
    // Return the best if one was found
    return min_url;
  }
  
  protected void enforcePolitenessDelay(URL url) {
    if (url == null)
      return;
   
    String hostname = url.getHost();
    long target;
    synchronized (nextFetchTimes) {
      // Remove pages visited more than the politeness delay ago
      long now = System.currentTimeMillis();
      Iterator<Map.Entry<String, HostRequests>> iterator = nextFetchTimes.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, HostRequests> entry = iterator.next();
        HostRequests host = entry.getValue();
        if (host.nextFetchTime < (now - host.delay) && host.outstanding == 0)
          iterator.remove();
      }
      
      // Find or create a HostRequests struct for this host
      HostRequests host = nextFetchTimes.get(hostname);
      if (host == null) {
        host = new HostRequests();
        host.delay = config.getPolitenessDelay();
      }
      
      ++host.outstanding;
      target = host.nextFetchTime;
      
      // The next fetch time is the current time + the politeness delay * the amount of
      // outstanding requests (= threads that are waiting to request a page from this host).
      // One additional std_delay is added in order to compensate for the response time of
      // the host. As soon as all requests have finished, the nextFetchTime is set
      // to the correct value.
      host.nextFetchTime = now + (host.delay * (host.outstanding + 1));
      
      // Remove penalty from the host once it is serving its politeness delay
      host.penalty = Math.max(0, host.penalty - host.delay);
      nextFetchTimes.put(hostname, host);
    }
     
    synchronized (this)
    {
      ++delay_counter;
      delay_total += (target - System.currentTimeMillis());
      double avg = Math.round(delay_total / delay_counter * 1000) / 1000.0;
      if (delay_last_time < System.currentTimeMillis() - 5000) {
        logger.info("Average politeness sleep: {} (averaged over {} units)", avg, delay_counter);
        delay_last_time = System.currentTimeMillis();
      }
    }
    
    long delay;
    while ((delay = target - System.currentTimeMillis()) > 0)
    {
      if (delay >= 300000)
      {
          logger.info("Sleeping 5 minutes for politeness delay for host {} - {} seconds remain afterwards", hostname, delay / 1000.0);
          delay = 300000;
      }
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e)
      {}
    }
  }
  
  /**
   * Mark a request to a host as finished. If this was the last outstanding request
   * for this host, the next fetch time is updated to now() + delay, otherwise
   * it remains untouched.
   * 
   * @param url The parsed URL that has been requested
   */
  private void finishRequest(URL url) {
    if (url == null)
      return;
    
    String hostname = url.getHost();
    synchronized (nextFetchTimes) {
      HostRequests host = nextFetchTimes.get(hostname);
      if (host != null) {
        if (host.outstanding < 1)
          logger.error("ERROR: outstanding requests for host {} was 0, when calling finishRequest", url.getHost());;
          
        host.outstanding = (host.outstanding > 1 ? host.outstanding - 1: 0);
        if (host.outstanding == 0)
          host.nextFetchTime = System.currentTimeMillis() + host.delay;
          
        nextFetchTimes.put(hostname, host);
      }
    }
  }
  
  private void doAuthetication(List<AuthInfo> authInfos) {
    for (AuthInfo authInfo : authInfos) {
      if (authInfo.getAuthenticationType() == AuthInfo.AuthenticationType.BASIC_AUTHENTICATION) {
        doBasicLogin((BasicAuthInfo) authInfo);
      } else if (authInfo.getAuthenticationType() == AuthInfo.AuthenticationType.NT_AUTHENTICATION) {
        doNtLogin((NtAuthInfo) authInfo);
      } else {
        doFormLogin((FormAuthInfo) authInfo);
      }
    }
  }

  /**
   * BASIC authentication<br/>
   * Official Example: https://hc.apache.org/httpcomponents-client-ga/httpclient/examples/org/apache/http/examples
   * /client/ClientAuthentication.java
   * */
  private void doBasicLogin(BasicAuthInfo authInfo) {
    logger.info("BASIC authentication for: " + authInfo.getLoginTarget());
    HttpHost targetHost = new HttpHost(authInfo.getHost(), authInfo.getPort(), authInfo.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
                                 new UsernamePasswordCredentials(authInfo.getUsername(), authInfo.getPassword()));
    httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
  }

  /**
   * Do NT auth for Microsoft AD sites.
   */
  private void doNtLogin(NtAuthInfo authInfo) {
    logger.info("NT authentication for: " + authInfo.getLoginTarget());
    HttpHost targetHost = new HttpHost(authInfo.getHost(), authInfo.getPort(), authInfo.getProtocol());
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    try {
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
              new NTCredentials(authInfo.getUsername(), authInfo.getPassword(),
                      InetAddress.getLocalHost().getHostName(), authInfo.getDomain()));
    } catch (UnknownHostException e) {
      logger.error("Error creating NT credentials", e);
    }
    httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
  }

  /**
   * FORM authentication<br/>
   * Official Example:
   *  https://hc.apache.org/httpcomponents-client-ga/httpclient/examples/org/apache/http/examples/client/ClientFormLogin.java
   */
  private void doFormLogin(FormAuthInfo authInfo) {
    logger.info("FORM authentication for: " + authInfo.getLoginTarget());
    String fullUri =
        authInfo.getProtocol() + "://" + authInfo.getHost() + ":" + authInfo.getPort() + authInfo.getLoginTarget();
    HttpPost httpPost = new HttpPost(fullUri);
    List<NameValuePair> formParams = new ArrayList<>();
    formParams.add(new BasicNameValuePair(authInfo.getUsernameFormStr(), authInfo.getUsername()));
    formParams.add(new BasicNameValuePair(authInfo.getPasswordFormStr(), authInfo.getPassword()));

    try {
      UrlEncodedFormEntity entity = new UrlEncodedFormEntity(formParams, "UTF-8");
      httpPost.setEntity(entity);
      httpClient.execute(httpPost);
      logger.debug("Successfully Logged in with user: " + authInfo.getUsername() + " to: " + authInfo.getHost());
    } catch (UnsupportedEncodingException e) {
      logger.error("Encountered a non supported encoding while trying to login to: " + authInfo.getHost(), e);
    } catch (ClientProtocolException e) {
      logger.error("While trying to login to: " + authInfo.getHost() + " - Client protocol not supported", e);
    } catch (IOException e) {
      logger.error("While trying to login to: " + authInfo.getHost() + " - Error making request", e);
    }
  }

  public PageFetchResult fetchPage(WebURL webUrl)
      throws InterruptedException, IOException, PageBiggerThanMaxSizeException {
    // Getting URL, setting headers & content
    PageFetchResult fetchResult = new PageFetchResult();
    String toFetchURL = webUrl.getURL();
    URL parsedUrl = null;
    try {
      parsedUrl = new URL(toFetchURL);
    } catch (MalformedURLException e)
    {}
    
    HttpUriRequest request = null;
    try {
      request = newHttpUriRequest(toFetchURL);
      // Applying Politeness delay
      enforcePolitenessDelay(parsedUrl);

      CloseableHttpResponse response = httpClient.execute(request);
      fetchResult.setEntity(response.getEntity());
      fetchResult.setResponseHeaders(response.getAllHeaders());
      
      // Setting HttpStatus
      int statusCode = response.getStatusLine().getStatusCode();

      // If Redirect ( 3xx )
      if (statusCode == HttpStatus.SC_MOVED_PERMANENTLY || statusCode == HttpStatus.SC_MOVED_TEMPORARILY ||
          statusCode == HttpStatus.SC_MULTIPLE_CHOICES || statusCode == HttpStatus.SC_SEE_OTHER ||
          statusCode == HttpStatus.SC_TEMPORARY_REDIRECT ||
          statusCode == 308) { // todo follow https://issues.apache.org/jira/browse/HTTPCORE-389

        Header header = response.getFirstHeader("Location");
        if (header != null) {
          String movedToUrl = URLCanonicalizer.getCanonicalURL(header.getValue(), toFetchURL);
          fetchResult.setMovedToUrl(movedToUrl);
        }
      } else if (statusCode >= 200 && statusCode <= 299) { // is 2XX, everything looks ok
        fetchResult.setFetchedUrl(toFetchURL);
        String uri = request.getURI().toString();
        if (!uri.equals(toFetchURL)) {
          if (!URLCanonicalizer.getCanonicalURL(uri).equals(toFetchURL)) {
            fetchResult.setFetchedUrl(uri);
          }
        }

        // Checking maximum size
        if (fetchResult.getEntity() != null) {
          long size = fetchResult.getEntity().getContentLength();
          if (size == -1) {
            Header length = response.getLastHeader("Content-Length");
            if (length == null) {
              length = response.getLastHeader("Content-length");
            }
            if (length != null) {
              size = Integer.parseInt(length.getValue());
            }
          }
          if (size > config.getMaxDownloadSize()) {
            response.close();
            throw new PageBiggerThanMaxSizeException(size);
          }
        }
      }

      fetchResult.setStatusCode(statusCode);
      return fetchResult;

    } finally { // occurs also with thrown exceptions
      if ((fetchResult.getEntity() == null) && (request != null)) {
        request.abort();
      }
      
      finishRequest(parsedUrl);
    }
  }

  public synchronized void shutDown() {
    if (connectionMonitorThread != null) {
      connectionManager.shutdown();
      connectionMonitorThread.shutdown();
    }
  }

  /**
   * Creates a new HttpUriRequest for the given url. The default is to create a HttpGet without
   * any further configuration. Subclasses may override this method and provide their own logic.
   *
   * @param url the url to be fetched
   * @return the HttpUriRequest for the given url
   */
  protected HttpUriRequest newHttpUriRequest(String url) {
    return new HttpGet(url);
  }

}
