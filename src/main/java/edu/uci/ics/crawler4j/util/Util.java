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

/**
 * @author Yasser Ganjisaffar
 */
public class Util {
  
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