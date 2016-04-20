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

package edu.uci.ics.crawler4j.frontier;

import java.net.URISyntaxException;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.uci.ics.crawler4j.url.WebURL;

/**
 * @author Yasser Ganjisaffar
 */
public class WebURLTupleBinding extends TupleBinding<WebURL> {
  @Override
  public WebURL entryToObject(TupleInput input) {
    WebURL webURL;
    try {
      webURL = new WebURL(input.readString());
    } catch (URISyntaxException e) {
      // If this happens, it means an invalid WebURL was entered to begin with,s
      // so something is seriously broken.
      throw new RuntimeException(e);
    }
    webURL.setDocid(input.readLong());
    webURL.setParentDocid(input.readLong());
    webURL.setParentUrl(input.readString());
    webURL.setSeedDocid(input.readLong());;
    webURL.setDepth(input.readShort());
    webURL.setPriority(input.readByte());
    webURL.setAnchor(input.readString());

    // Read previous key if it's stored, otherwise it's null
    boolean has_prev = input.readBoolean();
    if (has_prev) {
      byte [] prev = new byte[WebURL.KEY_SIZE];
      input.read(prev, 0, WebURL.KEY_SIZE);
      webURL.setPrevious(prev);
    } else {
      webURL.setPrevious((byte [])null);
    }
    
    // Read next key if it's stored, otherwise it's null
    boolean has_next = input.readBoolean();
    if (has_next) {
      byte [] next = new byte[WebURL.KEY_SIZE];
      input.read(next, 0, WebURL.KEY_SIZE);
      webURL.setNext(next);
    } else {
      webURL.setNext((byte []) null);
    }
    
    return webURL;
  }

  @Override
  public void objectToEntry(WebURL url, TupleOutput output) {
    output.writeString(url.getURL());
    output.writeLong(url.getDocid());
    output.writeLong(url.getParentDocid());
    output.writeString(url.getParentUrl());
    output.writeLong(url.getSeedDocid());
    output.writeShort(url.getDepth());
    output.writeByte(url.getPriority());
    output.writeString(url.getAnchor());
    
    // Write if there is a prev-key, and the key itself
    byte [] prev_key = url.getPrevious();
    output.writeBoolean(prev_key != null);
    if (prev_key != null)
      output.write(prev_key, 0, WebURL.KEY_SIZE);
    
    // Write if there is a next-key, and the key itself
    byte [] next_key = url.getNext();
    output.writeBoolean(next_key != null);
    if (next_key != null)
      output.write(next_key, 0, WebURL.KEY_SIZE);
  }
}