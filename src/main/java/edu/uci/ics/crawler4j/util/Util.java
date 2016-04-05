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


/**
 * @author Yasser Ganjisaffar
 */
public class Util {

  public static byte[] long2ByteArray(long l) {
    byte[] array = new byte[8];
    int i;
    int shift;
    for (i = 0, shift = 56; i < 8; i++, shift -= 8) {
      array[i] = (byte) (0xFF & (l >> shift));
    }
    return array;
  }

  public static byte[] int2ByteArray(int value) {
    byte[] b = new byte[4];
    for (int i = 0; i < 4; i++) {
      int offset = (3 - i) * 8;
      b[i] = (byte) ((value >>> offset) & 0xFF);
    }
    return b;
  }

  public static void putLongInByteArray(long value, byte[] buf, int offset) {
    for (int i = 0; i < 8; i++) {
      int valueOffset = (7 - i) * 8;
      buf[offset + i] = (byte) ((value >>> valueOffset) & 0xFF);
    }
  }
  
  public static void putIntInByteArray(int value, byte[] buf, int offset) {
    for (int i = 0; i < 4; i++) {
      int valueOffset = (3 - i) * 8;
      buf[offset + i] = (byte) ((value >>> valueOffset) & 0xFF);
    }
  }

  public static int byteArray2Int(byte[] b) {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      int shift = (4 - 1 - i) * 8;
      value += (b[i] & 0x000000FF) << shift;
    }
    return value;
  }

  public static long byteArray2Long(byte[] b) {
    int value = 0;
    for (int i = 0; i < 8; i++) {
      int shift = (8 - 1 - i) * 8;
      value += (b[i] & 0x000000FF) << shift;
    }
    return value;
  }

  public static boolean hasBinaryContent(String contentType, byte [] content) {
    String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

    if (typeStr.isEmpty())
    {
        try
        {
            // Try to parse content as UTF-8 to see if it's xml or xhtml+xml
            String str = new String(content, "UTF-8");
            if (str.startsWith("<?xml")) // xml should be treated as binary, but xhtml shouldn't
                return !str.contains("<html");
        }
        catch (Exception e)
        {}
        
        // No verdict - go count non-ascii characters to estimate probability on binary content
        int nonASCII = 0;
        int chars = Math.min(content.length, 2048);
        for (int i = 0; i < chars; ++i)
            if (content[i] < 32 || content[i] > 126)
                ++nonASCII;
        
        // If 10% of the characters are outside of ASCII range and no content type has been specified,
        // it's very likely that we're dealing with binary content here.
        return (nonASCII / (double)chars) > 0.1;
    }
    
    // xhtml is application/xhtml+xml - xhtml can be parsed by HTML parsed, but XML should be parsed by TIKA
    if (typeStr.contains("xml"))
        return !typeStr.contains("html");
    
    return typeStr.contains("image") || typeStr.contains("audio") || typeStr.contains("video") ||
           typeStr.contains("application");
  }

  public static boolean hasPlainTextContent(String contentType) {
    String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

    return typeStr.startsWith("text/plain");
  }

}