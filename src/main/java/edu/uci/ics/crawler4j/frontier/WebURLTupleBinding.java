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
    
    byte [] prev = new byte[URLQueue.KEY_SIZE];
    input.read(prev, 0, URLQueue.KEY_SIZE);
    webURL.setPrevious(prev);
    
    byte [] next = new byte[URLQueue.KEY_SIZE];
    input.read(next, 0, URLQueue.KEY_SIZE);
    webURL.setNext(next);
    
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
    output.write(url.getPrevious(), 0, URLQueue.KEY_SIZE);
    output.write(url.getNext(), 0, URLQueue.KEY_SIZE);
  }
}