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
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.parser.Parser;

/**
 * @author Yasser Ganjisaffar
 */
public class Util {

  protected static final Logger logger = LoggerFactory.getLogger(Util.class);

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
    ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES);
    bbuf.putLong(value);
    return bbuf.array();
  }

  public static byte[] int2ByteArray(int value) {
    ByteBuffer bbuf = ByteBuffer.allocate(Integer.BYTES);
    bbuf.putInt(value);
    return bbuf.array();
  }

  public static void putLongInByteArray(long value, byte[] buf, int offset) {
    ByteBuffer buf2 = ByteBuffer.wrap(buf, offset, Long.BYTES);
    buf2.putLong(value);
  }
  
  public static void putIntInByteArray(int value, byte[] buf, int offset) {
    ByteBuffer buf2 = ByteBuffer.wrap(buf, offset, Integer.BYTES);
    buf2.putInt(value);
  }

  public static int byteArray2Int(byte[] buf) {
    return extractIntFromByteArray(buf, 0);
  }

  public static long byteArray2Long(byte[] buf) {
    return extractLongFromByteArray(buf, 0);
  }

  public static int extractIntFromByteArray(byte[] b, int offset) {
    ByteBuffer bbuf = ByteBuffer.wrap(b, offset, Integer.BYTES);
    return bbuf.getInt();
  }
  
  public static long extractLongFromByteArray(byte[] b, int offset) {
    ByteBuffer bbuf = ByteBuffer.wrap(b, offset, Long.BYTES);
    return bbuf.getLong();
  }
  
  public enum ContentType {
    TEXT, HTML, XML, BINARY
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
    // First check if contentType is text/plain to filter out the robots.txt
    if (contentType.contains("text/plain"))
      return ContentType.TEXT;

    // Check the byte order marker
    String UTFType = checkByteOrderMarker(content);
    String stringContent;
    try {
      stringContent = new String(content, UTFType);
    } catch (UnsupportedEncodingException e) {
      return ContentType.BINARY;
    }

    // Check the non ascii for the UTF-8
    if (checkNonAscii(content) && (!(UTFType.equals("UTF-16") || UTFType.equals("UTF-32")))) {
      return ContentType.BINARY;
    } else {
      if (hasHTMLContent(stringContent)) {
        return ContentType.HTML;
      } else if (hasXMLContent(stringContent)) {

        return ContentType.XML;
      } else {
        return ContentType.TEXT;
      }
    }
  }

  /**
   * Detect if the document has is of the type html
   *
   * @param str:
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
   * @param str:
   *          the content string
   *
   * @return true if it of the type of XML
   */
  public static boolean hasXMLContent(String str) {
    if (str.startsWith("<?xml"))
      return true;
    return false;
  }

  /**
   * 
   * @param content
   *          : content of the document
   * @return the type of UTF
   */

  public static String checkByteOrderMarker(byte[] content) {
    // Parse the integer to the hex string
    // Check for the UTF-32. UTF-32 is either 0 0 FE FF/ 0 0 FF FE
    if (content[0] == 0 && content[1] == 0) {
      if (((content[2] & 0xFF) == 0xFF && (content[3] & 0xFE) == 0xFE)
          || ((content[2] & 0xFE) == 0xFE && (content[3] & 0xFF) == 0xFF)) {
        return "UTF-32";
      }
    }
    // Check for the UTF-32 and UTF-16. UTF-32 is either FE FF 0 0 / FF FE 0 0
    // And UTF-16 is FE FF or FE FF.

    if (((content[0] & 0xFF) == 0xFF && (content[1] & 0xFE) == 0xFE)
        || ((content[0] & 0xFE) == 0xFE && (content[1] & 0xFF) == 0xFF)) {

      if (content[2] == 0 && content[3] == 0) {

        return "UTF-32";
      }
      return "UTF-16";
    }
    return "UTF-8";
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

  public static boolean checkNonAscii(byte[] content) {
    // No verdict - go count non-ascii characters to estimate
    // probability on binary content
    int nonASCII = 0;
    int chars = Math.min(content.length, 2048);
    for (int i = 0; i < chars; ++i)
      if (content[i] < 32 || content[i] > 126)
        ++nonASCII;

    // If 10% of the characters are outside of ASCII range and no
    // content type has been specified,
    // it's very likely that we're dealing with binary content here.
    return (nonASCII / (double) chars) > 0.1;
  }
}
