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
import java.util.Arrays;
import java.util.List;

/**
 * @author Yasser Ganjisaffar
 */
public class Util
{
    static List<String> CONTENT_TYPE_WHITELIST = Arrays.asList("text/plain", // Plain
                                                                             // text
            "text/html", // HTML
            "application/xhtml+xml", // XHTML
            "application/pdf", // PDF
            "application/msword", // .doc (Word)
            "application/vnd.oasis.opendocument.text", // .odt (LibreOffice
                                                       // Writer)
            "application/vnd.oasis.opendocument.presentation", // .odp
                                                               // (LibreOffice
                                                               // Impress)
            "application/vnd.sun.xml.writer", // .sxw (OpenOffice Writer)
            "application/vnd.sun.xml.writer.global", // .sxg (OpenOffice Writer)
            "application/vnd.sun.xml.impress", // .sxi (OpenOffice Impress)
            "application/vnd.ms-powerpoint", // .ppt (Powerpoint)
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document", // .docx
                                                                                       // (Word)
            "application/vnd.openxmlformats-officedocument.presentationml.presentation" // Powerpoint
                                                                                        // .pptx
    );

    public static class Reference<T>
    {
        public T val;

        public Reference(T val)
        {
            this.val = val;
        }

        public T assign(T val)
        {
            return this.val = val;
        }

        public T get()
        {
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
        if (hasHTMLContent(contentType, content))
        {
            return "Html";
        }
        else if (hasWhiteListContent(contentType))
        {
            return "WhiteList";
        }
        else if (hasXMLContent(contentType, content))
        {
            if (hasHTMLContent(contentType, content))
                return "Html";
            else
                return "XML";
        }
        else if (hasBinaryContent(contentType, content))
        {
            return "Binary";
        }
        else if (hasPlainTextContent(contentType))
        {
            return "Plaintext";
        }
        return "Unknown";
    }

    /**
     * Detect if the document has is of the type html
     *
     * @param contentType
     *            the type where the site is addressed with
     * @param content
     *            the text of the site
     * @return true if it is a html site and false when it is not
     */
    public static boolean hasHTMLContent(String contentType, byte[] content)
    {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";
        try
        {
            String str = "";
            if (contentType.contains("UTF-16") || content.toString().startsWith("0xFE")
                    || content.toString().startsWith("0xFF"))
            {
                str = new String(content, "UTF-16");
            }
            else if (contentType.contains("UTF-32"))
            {
                str = new String(content, "UTF-32");
            }
            else
            {
                str = new String(content, "UTF-8");
            }
            if (str.contains("<html") || typeStr.contains("text/html"))
                return true;
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Detect of content is of type pdf
     *
     * @param contentType
     *            the type of the content
     * @return true if the content is of type pdf and false when it is not
     */

    public static boolean hasWhiteListContent(String contentType)
    {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";
        for (String whiteListContentType : CONTENT_TYPE_WHITELIST)
        {
            if (typeStr.startsWith(whiteListContentType))
                return true;
        }
        return false;
    }

    /**
     * Detect if the document is of the type of XML
     *
     * @param contentType
     *            the type of the document
     * @param content
     *            the text in the document
     * @return true if it of the type of XML
     */
    public static boolean hasXMLContent(String contentType, byte[] content)
    {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";
        try
        {
            // Try to parse content as UTF-8 to see if it's xml or xhtml+xml
            String str = new String(content, "UTF-8");
            if (str.startsWith("<?xml") || typeStr.contains("xml"))
                return true;
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Detect if the document is of the type of binary so application or audio
     * etc
     * 
     * @param contentType
     *            the type where the document is addressed with
     * @param content
     *            the text on the document
     * @return true if the document is of the type of binary and false whenever
     *         it is not
     */
    public static boolean hasBinaryContent(String contentType, byte[] content)
    {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

        if (typeStr.isEmpty())
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

        return typeStr.contains("image") || typeStr.contains("audio") || typeStr.contains("video")
                || typeStr.contains("application");
    }

    /**
     * Detect if the document is of type plain text
     * 
     * @param contentType
     *            the type where the document is addressed with
     * @return true if the document is of the type of plain text
     */
    public static boolean hasPlainTextContent(String contentType)
    {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";
        return typeStr.startsWith("text/plain");
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
