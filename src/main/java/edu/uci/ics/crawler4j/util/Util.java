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
public class Util
{

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

    public static byte[] long2ByteArray(long l)
    {
        byte[] array = new byte[8];
        int i;
        int shift;
        for (i = 0, shift = 56; i < 8; i++, shift -= 8)
        {
            array[i] = (byte) (0xFF & (l >> shift));
        }
        return array;
    }

    public static byte[] int2ByteArray(int value)
    {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++)
        {
            int offset = (3 - i) * 8;
            b[i] = (byte) ((value >>> offset) & 0xFF);
        }
        return b;
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
  
    /**
     * Detect the content type of a document
     * 
     * @param contentType
     *            is the content type that is addressed to the document
     * @param content
     *            is the text of the document
     * @return the type of the document
     */
    public static String getContentType(String contentType, byte[] content)
    {
        //First check if contentType is text/plain to filter out the robots.txt
        if (contentType.contains("text/plain"))
            return "Plaintext";

        //Check the byte order marker
        String UTFType = checkByteOrderMarker(content);
        String stringContent;
        try
        {
            stringContent = new String(content, UTFType);
        }
        catch (UnsupportedEncodingException e)
        {
            return "Binary";
        }
        
        //Check the non ascii for the UTF-8
        if (checkNonAscii(content) && (!(UTFType.equals("UTF-16") || UTFType.equals("UTF-32"))))
        {
            return "Binary";
        }
        else
        {
            if (hasHTMLContent(stringContent))
            {
                return "Html";
            }
            else if (hasXMLContent(stringContent))
            {

                return "XML";
            }
            else
            {
                return "Plaintext";
            }
        }
    }

    /**
     * Detect if the document has is of the type html
     *
     * @param str: the content string
     *
     * @return true if it is a html document and false when it is not
     */
    public static boolean hasHTMLContent(String str)
    {    
        if(str.contains("<!doctype") || str.contains("<html")|| str.contains("<body")||str.contains("<head"))
                return true;
        return false;
    }

    /**
     * Detect if the document is of the type of XML
     *
     * @param str: the content string
     *
     * @return true if it of the type of XML
     */
    public static boolean hasXMLContent(String str)
    {
            if (str.startsWith("<?xml"))
                return true;
        return false;
    }

/**
 * 
 * @param content : content of the document
 * @return the type of UTF
 */
    
    public static String checkByteOrderMarker(byte[] content)
    {
        //Parse the integer to the hex string
        String hexString0 = Integer.toHexString(content[0]);
        String hexString1 = Integer.toHexString(content[1]);
        String hexString2 = Integer.toHexString(content[2]);
        String hexString3 = Integer.toHexString(content[3]);
        
        //Check for the UTF-32. UTF-32 is either 0 0 FE FF/ 0 0 FF FE
        if (hexString0.equals("0") && hexString1 == "0")
        {
            if ((hexString2.equals("ffffffff") && hexString3.equals("fffffffe"))
                    || (hexString2.equals("fffffffe") && hexString3.equals("ffffffff")))
            {
                return "UTF-32";
            }
        }
        //Check for the UTF-32 and UTF-16. UTF-32 is either FE FF 0 0 / FF FE 0 0
        //And UTF-16 is FE FF or FE FF.
        if ((hexString0.equals("ffffffff") && hexString1.equals("fffffffe"))
                || (hexString0.equals("fffffffe") && hexString1.equals("ffffffff")))
        {
            if (hexString2.equals("0") && hexString3.equals("0"))
                return "UTF-32";
            return "UTF-16";
        }
        return "UTF-8";
    }
    
    
    public static boolean checkNonAscii(byte[] content)
    {
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
}
