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

package edu.uci.ics.crawler4j.url;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

import edu.uci.ics.crawler4j.util.Util;

/**
 * @author Yasser Ganjisaffar
 */

@Entity
public class WebURL implements Serializable, Comparable<WebURL> {

  private static final long serialVersionUID = 1L;

  @PrimaryKey
  private String url;
  private URI uri;

  private long docid = -1;
  private long parentDocid;
  private long seedDocid;
  private String parentUrl;
  private short depth;
  private String domain;
  private String subDomain;
  private String path;
  private String anchor;
  private byte priority = 0;
  private String tag;
  private boolean seedEnded = false;
  
  private byte [] host_previous_url = null;
  private byte [] host_next_url = null;
  private byte [] keyData = null;

  public static final int KEY_SIZE = 10;

  /** Copy constructor
   * 
   * @param rhs The WebURL to copy
   **/
  public WebURL(WebURL rhs)
  {
    this.url = rhs.url;
    this.uri = rhs.uri;
    this.docid = rhs.docid;
    this.parentDocid = rhs.parentDocid;
    this.seedDocid = rhs.seedDocid;
    this.parentUrl = rhs.parentUrl;
    this.depth = rhs.depth;
    this.domain = rhs.domain;
    this.subDomain = rhs.subDomain;
    this.path = rhs.path;
    this.anchor = rhs.anchor;
    this.priority = rhs.priority;
    this.tag = rhs.tag;
    this.seedEnded = rhs.seedEnded;
    this.host_previous_url = rhs.host_previous_url;
    this.host_next_url = rhs.host_next_url;
  }

  public WebURL(String url) throws URISyntaxException {
      setURL(url);
  }

  public String getProtocol() {
    return uri != null ? uri.getScheme().toLowerCase() : null;
  }
  
  public boolean isHttp() {
    String protocol = getProtocol();
    return protocol != null && (protocol.equals("http") || protocol.equals("https"));
  }
  
  public URI getURI() {
    return uri;
  }
  
  /**
   * @return unique document id assigned to this Url.
   */
  public long getDocid() {
    return docid;
  }

  public void setDocid(long docid) {
    this.docid = docid;
  }

  /**
   * @return Url string
   */
  public String getURL() {
    return url;
  }

  public void setURL(String url) throws URISyntaxException {
    this.url = url;
    this.uri = new URI(url);
    
    domain = uri.getHost();
    subDomain = "";
    
    if (domain == null) {
      throw new URISyntaxException(url, "Domain is null");
    }
    String[] parts = domain.split("\\.");
    if (parts.length > 2) {
      domain = parts[parts.length - 2] + "." + parts[parts.length - 1];
      int limit = 2;
      if (TLDList.getInstance().contains(domain)) {
        domain = parts[parts.length - 3] + "." + domain;
        limit = 3;
      }
      for (int i = 0; i < (parts.length - limit); i++) {
        if (!subDomain.isEmpty()) {
          subDomain += ".";
        }
        subDomain += parts[i];
      }
    }
    path = uri.getPath();
  }

  /**
   * @return
   *      unique document id of the parent page. The parent page is the
   *      page in which the Url of this page is first observed.
   */
  public long getParentDocid() {
    return parentDocid;
  }

  public void setParentDocid(long parentDocid) {
    this.parentDocid = parentDocid;
  }
  
  /**
   * Returns the unique document id of the seed page. The seed page is the
   * page that was used as the starting point that eventually resulted in this
   * page being visited.
   * 
   * @return The Docid of the seed
   */
  public long getSeedDocid() {
      return seedDocid;
  }
  
  public void setSeedDocid(long seedDocid) {
      this.seedDocid = seedDocid;
  }

  /**
   * Return whether the seed has ended. This is used to signal to the
   * WebCrawler that handleSeedEnd should be called on this seed, but
   * that the page itself should not be crawled anymore.
   * 
   * @return Whether the seed has ended
   */
  public boolean getSeedEnded() {
      return seedEnded;
  }
  
  public void setSeedEnded(boolean seedEnded) {
      this.seedEnded = seedEnded;
  }
  
  /**
   * @return
   *      url of the parent page. The parent page is the page in which
   *      the Url of this page is first observed.
   */
  public String getParentUrl() {
    return parentUrl;
  }

  public void setParentUrl(String parentUrl) {
    this.parentUrl = parentUrl;
  }

  /**
   * @return
   *      crawl depth at which this Url is first observed. Seed Urls
   *      are at depth 0. Urls that are extracted from seed Urls are at depth 1, etc.
   */
  public short getDepth() {
    return depth;
  }

  public void setDepth(short depth) {
    this.depth = depth;
  }

  /**
   * @return
   *      domain of this Url. For 'http://www.example.com/sample.htm', domain will be 'example.com'
   */
  public String getDomain() {
    return domain;
  }

  public String getSubDomain() {
    return subDomain;
  }

  /**
   * @return
   *      path of this Url. For 'http://www.example.com/sample.htm', domain will be 'sample.htm'
   */
  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  /**
   * @return
   *      anchor string. For example, in <a href="example.com">A sample anchor</a>
   *      the anchor string is 'A sample anchor'
   */
  public String getAnchor() {
    return anchor;
  }

  public void setAnchor(String anchor) {
    this.anchor = anchor;
  }

  /**
   * @return priority for crawling this URL. A lower number results in higher priority.
   */
  public byte getPriority() {
    return priority;
  }

  public void setPriority(byte priority) {
    this.priority = priority;
  }

  /**
   * @return tag in which this URL is found
   * */
  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  @Override
  public int hashCode() {
    return url.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }

    WebURL otherUrl = (WebURL) o;
    return (url != null) && url.equals(otherUrl.getURL());

  }

  @Override
  public String toString() {
    return "WebURL: " + url + " (did:" + docid + " pid:" + parentDocid + " sid:" + seedDocid + " pr:" + priority + " dp:" + depth + ")";
  }
  
  public void setPrevious(WebURL previous_url) {
    setPrevious(previous_url != null ? previous_url.getKey() : null);
  }
    
  public void setPrevious(byte [] previous_key) {
    this.host_previous_url = previous_key;
  }
  
  public byte [] getPrevious() {
    return this.host_previous_url;
  }
  
  public void setNext(byte [] next_key) {
    this.host_next_url = next_key;
  }
  
  public void setNext(WebURL next_url) {
    setNext(next_url != null ? next_url.getKey() : null);
  }
  
  public byte [] getNext() {
    return this.host_next_url;
  }
  
  /**
   * The key that is used for storing URLs determines the order
   * they are crawled. Lower key values results in earlier crawling.
   * Here our keys are 10 bytes. The first byte comes from the URL priority.
   * The second byte comes from depth of crawl at which this URL is first found.
   * The remaining 8 bytes come from the docid of the URL. As a result,
   * URLs with lower priority numbers will be crawled earlier. If priority
   * numbers are the same, those found at lower depths will be crawled earlier.
   * If depth is also equal, those found earlier (therefore, smaller docid) will
   * be crawled earlier.
   * 
   * @return The 10-byte database key
   */
  public synchronized byte [] getKey() {
    if (keyData != null)
      return keyData;
    
    keyData = createKey(priority, depth, docid);
    return keyData;
  }
  
  public static byte [] createKey(byte priority, short depth, long docid) {
    byte [] keyData = new byte[WebURL.KEY_SIZE];
    
    // Because the ordering is done strictly binary, negative values will come last, because
    // their binary representation starts with the MSB at 1. In order to fix this, we'll have
    // to add the minimum value to become 0. This means that the maximum number will become
    // out of range in Byte-value, but the integer value is nicely converted down to the actual
    // binary representation that is useful here.
    byte binary_priority = (byte)(priority - Byte.MIN_VALUE);
    keyData[0] = binary_priority;
    keyData[1] = (depth > Byte.MAX_VALUE ? Byte.MAX_VALUE : (byte) depth);
    
    Util.putLongInByteArray(docid, keyData, 2);
    return keyData;
  }
  
  /**
   * Compare the WebURL with a different one, based on the key determined above
   * 
   * @param rhs The URL to compare with
   * @return -1 if the current URL comes before rhs, 1 if it comes after and 0 if they are equal
   */
  public int compareTo(WebURL rhs) {
    return compareKey(getKey(), rhs.getKey());
  }
  
  /**
   * Compare the WebURL's key with a different key
   * @param lhs Left hand side of the comparison
   * @param rhs Right hand side of the comparison
   *  
   * @return -1 if the current URL comes before key, 1 if it comes after and 0 if they are equal
   */ 
  public static int compareKey(byte [] lhs, byte [] rhs) {
    for (int i = 0; i < KEY_SIZE; ++i)
      if (lhs[i] != rhs[i])
        return (lhs[i] & 0xFF) - (rhs[i] & 0xFF);
    
    return 0;
  }
}
