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

package edu.uci.ics.crawler4j.robotstxt;

import java.util.TreeSet;

/**
 * @author Yasser Ganjisaffar
 */
public class HostDirectives {
  // If we fetched the directives for this host more than
  // 24 hours, we have to re-fetch it.
  private static final long EXPIRATION_DELAY = 24 * 60 * 1000L;
  
  public static final int ALLOWED = 1;
  public static final int DISALLOWED = 2;
  public static final int UNDEFINED = 3;
  
  /** A list of rule sets, sorted on match with the configured user agent */
  private final TreeSet<UserAgentDirectives> rules;
  
  private final long timeFetched;
  private long timeLastAccessed;
  private RobotstxtConfig config;
  private String userAgent;

  public HostDirectives(RobotstxtConfig configuration) {
    timeFetched = System.currentTimeMillis();
    config = configuration;
    userAgent = config.getUserAgentName();
    rules = new TreeSet<UserAgentDirectives>(new UserAgentDirectives.UserAgentComparator(userAgent));
  }

  public boolean needsRefetch() {
    return ((System.currentTimeMillis() - timeFetched) > EXPIRATION_DELAY);
  }

  /**
   * Check if the host directives allows visiting path.
   * 
   * @param path The path to check
   * @return True if the path is not disallowed, false if it is
   */
  public boolean allows(String path) {
    return checkAccess(path) != DISALLOWED;
  }
  
  /**
   * Check if the host directives explicitly disallow visiting path.
   * 
   * @param path The path to check
   * @return True if the path is explicity disallowed, false otherwise
   */
  public boolean disallows(String path) {
    return checkAccess(path) == DISALLOWED;
  }
  
  /**
   * Check if any of the rules say anything about the specified path
   * 
   * @param path The path to check
   * @return One of ALLOWED, DISALLOWED or UNDEFINED
   */
  public int checkAccess(String path) {
    timeLastAccessed = System.currentTimeMillis();
    int result = UNDEFINED;
    String myUA = config.getUserAgentName();
    boolean ignoreUA = config.getIgnoreUserAgentInAllow();
    for (UserAgentDirectives ua : rules) {
      boolean matchingUA = myUA.contains(ua.userAgent) && !ua.userAgent.equals("*");
      
      // If ignoreUA is disabled and the current UA doesn't match, the rest will not match
      // so we are done here.
      if (!matchingUA && !ignoreUA)
        break;
    
      // Match the rule to the path
      result = ua.checkAccess(path);
      
      // If the rule allows the path, we have our answer
      if (result == ALLOWED)
        break;
      // If the rule disallows the path, check if the UA actually matches, because
      // we could also have gotten here because ignoreUA is true
      else if (result == DISALLOWED && matchingUA)
        break;
      
      // Rule didn't apply based on the configuration, or did not state anything
      // about the current path.
      result = UNDEFINED;
    }
    return result;
  }
  
  /** 
   * Get a ruleset for the specified user agent. If it does not exist yet,
   * it will be created.
   *
   * @param userAgent The name of the user agent definition
   * @return The UserAgentRules directives
   */
  public UserAgentDirectives getDirectives(String userAgent) {
    UserAgentDirectives directives = new UserAgentDirectives(userAgent, this.userAgent);
    if (!rules.add(directives)) {
      for (UserAgentDirectives ua : rules) {
        if (ua.userAgent.equals(userAgent))
          return ua;
      }
    }
    
    return directives;
  }
  
  public long getLastAccessTime() {
    return timeLastAccessed;
  }
}