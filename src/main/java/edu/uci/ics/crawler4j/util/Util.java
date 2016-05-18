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

package edu.uci.ics.crawler4j.util;

import java.nio.ByteBuffer;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yasser Ganjisaffar
 */
public class Util {
  protected static final Logger logger = LoggerFactory.getLogger(Util.class);

  public enum ContentType {
    TEXT, HTML, XML, BINARY
  }

  public static class Reference<T> {
    public T val;

    public Reference(T val) {
      this.val = val;
    }

    public T assign(T val) {
      return this.val = val;
    }

    public T get() {
      return val;
    }
  }

  public static byte[] long2ByteArray(long value) {
    ByteBuffer bbuf = ByteBuffer.allocate(8);
    bbuf.putLong(value);
    return bbuf.array();
  }

  public static byte[] int2ByteArray(int value) {
    ByteBuffer bbuf = ByteBuffer.allocate(4);
    bbuf.putInt(value);
    return bbuf.array();
  }

  public static void putLongInByteArray(long value, byte[] buf, int offset) {
    ByteBuffer buf2 = ByteBuffer.wrap(buf, offset, 8);
    buf2.putLong(value);
  }
  
  public static void putIntInByteArray(int value, byte[] buf, int offset) {
    ByteBuffer buf2 = ByteBuffer.wrap(buf, offset, 4);
    buf2.putInt(value);
  }

  public static int byteArray2Int(byte[] buf) {
    return extractIntFromByteArray(buf, 0);
  }

  public static long byteArray2Long(byte[] buf) {
    return extractLongFromByteArray(buf, 0);
  }

  public static int extractIntFromByteArray(byte[] b, int offset) {
    ByteBuffer bbuf = ByteBuffer.wrap(b, offset, 4);
    return bbuf.getInt();
  }
  
  public static long extractLongFromByteArray(byte[] b, int offset) {
    ByteBuffer bbuf = ByteBuffer.wrap(b, offset, 8);
    return bbuf.getLong();
  }
  
  /**
   * Detect the content type of a document
   * 
   * @param contentType
   *          is the content type that is addressed to the document
   * @param content
   *          is the text of the document
   * @return the type of the document
   */
  public static ContentType getContentType(String contentType, byte[] content) {
    // Check if we may be dealing with a binary content-type
    String encoding_header = null;
    if (contentType != null) {
      int pos = contentType.indexOf("/");
      if (pos > 0) {
        String type = contentType.substring(0, pos);
        int end_pos = contentType.indexOf(";", pos);
        if (end_pos == -1)
          end_pos = contentType.indexOf(" ", pos);
        if (end_pos == -1)
          end_pos = contentType.length();
        
        String subtype = contentType.substring(pos + 1, end_pos);
        if (end_pos > 0 && end_pos < contentType.length()) { // Encoding is probably specified
          String encoding_str = contentType.substring(end_pos + 1).trim();
          if (encoding_str.startsWith("charset="))
            encoding_header = encoding_str.substring(8);
          else if (encoding_str.startsWith("encoding="))
            encoding_header = encoding_str.substring(9);
        }
        
        if (
            !type.equals("text")         // Text detection performed below
            && !subtype.equals("xml")    // Generic XML type (application/xml) 
            && !subtype.contains("+xml") // +xml means a format using XML, such as application/rss+xml
        ) {
          return ContentType.BINARY;
        }
      }
    }

    // PDF is text-like, so perform custom detection for %PDF header
    if (contentType == null && content[0] == '%' && content[1] == 'P' && content[2] == 'D' && content[3] == 'F') {
      return ContentType.BINARY;
    }
      
    // Perform text and encoding detection
    
    // First check for a byte order marker
    String stringContent = null;
    String encoding = detectEncoding(content);
    
    // Use encoding specified in header when detection failed
    if (encoding == null && encoding_header != null)
      encoding = encoding_header;
    
    if (encoding == null) {
      // It appears the character set could not be detected. We'll assume
      // that, based on the absence of a Byte Order Marker, the character
      // set is at least not UTF-16 or UTF-32.
      if (checkIfBinary(content))
        return ContentType.BINARY;
    
      // It seems there's a decent amount of ASCII-characters in there,
      // so let's try to decode it using UTF-8
      encoding = "UTF-8";
    }
    
    if (stringContent == null) {
      try {
        stringContent = new String(content, encoding);
      } catch (UnsupportedEncodingException e) {
        return ContentType.BINARY;
      }
    }
    
    if (hasHTMLContent(stringContent)) {
      return ContentType.HTML;
    } else if (hasXMLContent(stringContent)) {
      return ContentType.XML;
    } else {
      return ContentType.TEXT;
    }
  }

  /**
   * Detect if the document has is of the type html
   *
   * @param str
   *          the content string
   *
   * @return true if it is a html document and false when it is not
   */
  public static boolean hasHTMLContent(String str) {
    str = str.toLowerCase();
    if (str.contains("<!doctype") || str.contains("<html") || str.contains("<body") || str.contains("<head"))
      return true;
    return false;
  }

  /**
   * Detect if the document is of the type of XML
   *
   * @param str the content string
   *
   * @return true if it of the type of XML
   */
  public static boolean hasXMLContent(String str) {
    if (str.startsWith("<?xml"))
      return true;
    return false;
  }

  /**
   * Determine the character encoding of the provided byte content
   * 
   * @param content The content of the document
   * @return The type of encoding
   */
  public static String detectEncoding(byte[] content) {
    // First check with the universalchardet api if there is an encoding
    org.mozilla.universalchardet.UniversalDetector detector = new org.mozilla.universalchardet.UniversalDetector(null);
    detector.handleData(content, 0, content.length);
    detector.dataEnd();
    
    return detector.getDetectedCharset();
  }

  /**
   * Sleep for a minimum number of milliseconds, ignoring
   * InterruptedException.
   * 
   * @param ms The minimum number of milliseconds to sleep
   */
  public static void sleep(long ms) {
    if (ms <= 0)
      return;
    
    sleepUntil(System.currentTimeMillis() + ms);
  }
  
  /**
   * Sleep at least until a specific target time, ignoring InterruptedException
   * 
   * @param target The time in milliseconds specifying the minimum time that 
   *               this method will return.
   */
  public static void sleepUntil(long target) {
    long remain;
    while ((remain = target - System.currentTimeMillis()) > 0) {
      try {
        Thread.sleep(remain);
      } catch (InterruptedException e) {}
    }
  }
  
  /**
   * Sleep at least until a specific target time, ignoring InterruptedException
   * 
   * @param o The object on which to wait for a notification
   * @param max The maximum time to wait on the object in milliseconds
   * @return The actual amount of milliseconds actually spent waiting
   */
  public static long wait(Object o, long max) {
    synchronized (o) {
      long start = System.currentTimeMillis();
      try {
        o.wait(max);
      } catch (InterruptedException e) {}
      
      return System.currentTimeMillis() - start; 
    }
  }

  /**
   * Estimate if the content is binary or not. This is done by cheking the characters
   * in the first 2048 bytes. It is assumed that it is already known that
   * the content is not UTF-16 or UTF-16 by checking the byte order marker.
   * If ASCII-0 occurs, it is deemed to be binary. If not, then other ASCII control
   * characters are counted (below 0x20), and if they are more than 10 percent,
   * the content is deemed to be binary.
   * 
   * @param content The content to check
   * @return True if the content seems like binary content, false otherwise
   */
  public static boolean checkIfBinary(byte[] content) {
    // Count control characters characters to estimate probability on binary content
    int nonASCII = 0;
    int chars = Math.min(content.length, 2048);
    for (int i = 0; i < chars; ++i) {
      // ASCII-0 never occurs in text that is not UTF-16 or UTF-32
      int ch = content[i] & 0xFF;
      if (ch == 0)
        return true;
      
      // Count characters below 0x20 (space), because they should occur
      // very rarely in normal text.
      if (ch < 0x20)
        ++nonASCII;
    }

    // If 10% of the characters are control characters and no
    // content type has been specified, it's very likely that
    // we're dealing with binary content here.
    return (nonASCII / (double) chars) > 0.1;
  }
}
