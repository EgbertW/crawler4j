package edu.uci.ics.crawler4j.robotstxt;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The UserAgentDirectives class stores the configuration for a single
 * user agent as defined in the robots.txt. The user agent string used
 * depends on the most recent User-agent: definition in the robots.txt file.
 */
public class UserAgentDirectives {
  public static final Logger logger = LoggerFactory.getLogger(UserAgentDirectives.class);

  public String crawlUserAgent;
  public String userAgent;
  private List<String> sitemap = null;
  private String preferred_host = null;
  private Double crawl_delay = null;
  private Set<PathRule> path_rules = new HashSet<PathRule>();
  
  /**
   * Comparator used to order the list of matching path rules in such a way
   * that the most specific match (= longest) match comes first.
   */
  static class PathComparator implements Comparator<PathRule> {
    /** The path to compare the path rules with */
    String path;
    
    /** Initialize with the path */
    PathComparator(String path) {
      this.path = path;
    }
    
    /**
     * Compare two paths.
     * If lhs matches and rhs does not, this will return -1
     * If rhs matches and lhs does not, this will return 1
     * If both match or both do not match,, this will return the result of 
     *    a numeric comparison of the length of both patterns, where
     *    the longest (=most specific) one will come first.
     */
    @Override
    public int compare(PathRule lhs, PathRule rhs) {
      boolean p1_match = lhs.matches(path);
      boolean p2_match = rhs.matches(path);
      
      // Matching patterns come first
      if (p1_match && !p2_match) {
        return -1;
      } else if (p2_match && !p1_match) {
        return 1;
      }
      
      // Most specific pattern first
      String p1 = lhs.pattern.toString();
      String p2 = rhs.pattern.toString();
      
      if (p1.length() != p2.length())
        return Integer.compare(p2.length(), p1.length());
      
      // Just order alphabetically if the patterns are of the same length
      return p1.compareTo(p2);
    }
  }
  
  /** 
   * Create a UserAgentDirectives clause
   * @param userAgent The user agent for this rule
   */
  public UserAgentDirectives(String userAgent, String crawlUserAgent) {
    this.userAgent = userAgent;
    this.crawlUserAgent = crawlUserAgent;
  }
  
  public int checkAccess(String path) {
    // If the user agent does not match, the verdict is known
    if (!userAgent.equals("*") && !crawlUserAgent.contains(userAgent))
      return HostDirectives.UNDEFINED;
    
    // Order the rules based on their match with the path
    TreeSet<PathRule> rules = new TreeSet<PathRule>(new PathComparator(path));
    rules.addAll(path_rules);
    
    // Return the verdict of the best matching rule
    for (PathRule rule : rules) {
      if (rule.matches(path))
        return rule.type;
    }
    
    return HostDirectives.UNDEFINED;
  }
  
  public static class UserAgentComparator implements Comparator<UserAgentDirectives> {
    String crawlUserAgent;
      
    UserAgentComparator(String myUA) {
      crawlUserAgent = myUA;
    }
    
    @Override
    public int compare(UserAgentDirectives lhs, UserAgentDirectives rhs)
    {
      // Simple case: user agents are equal
      if (lhs.userAgent.equals(rhs.userAgent))
        return 0;
    
      boolean o1_contains = crawlUserAgent.contains(lhs.userAgent);
      boolean o2_contains = crawlUserAgent.contains(rhs.userAgent);
      
      if (!o1_contains && !o2_contains)
      {
        // Both user-agents do not match. However, one of them could be a wildcard.
        // As these, by definition, match the UA, they should be preferred 
        // to completely non-matching rules
        if (lhs.userAgent.equals("*"))
        {
          if (rhs.userAgent.equals("*"))
          return 0;
        }
        else if (rhs.userAgent.equals("*"))
          return 1;
        
        // Just use alphabetic ordering
        return lhs.userAgent.compareTo(rhs.userAgent);
      }
    
      // Check if either of them, but not both, match the UA.
      // In that case, prefer the matching UA
      if (o1_contains && !o2_contains)
        return -1;
      else if (!o1_contains && o2_contains)
        return 1;
    
      // Both match the UA. Find the best matching UA (== the longest match)
      return Integer.compare(rhs.userAgent.length(), lhs.userAgent.length());
    }
  }
  
  /**
   * Add a rule to the list of rules for this user agent.
   * Valid rules are: sitemap, crawl-delay, host, allow and disallow.
   * These are based on the wikipedia article at:
   * 
   * https://en.wikipedia.org/wiki/Robots_exclusion_standard
   * 
   * and the Google documentation at:
   * 
   * https://support.google.com/webmasters/answer/6062596
   * 
   * @param rule The name of the rule
   * @param value The value of the rule
   */
  public void add(String rule, String value)
  {
    if (rule.equals("sitemap")) {
      if (this.sitemap == null)
        this.sitemap = new ArrayList<String>();
      this.sitemap.add(value);
    } else if (rule.equals("crawl-delay")) {
      try {
        this.crawl_delay = Double.parseDouble(value);
      } catch (NumberFormatException e) {
        logger.warn("Invalid number format for crawl-delay robots.txt: {}", value);
      }
    } else if (rule.equals("host")) {
      this.preferred_host = value;
    } else if (rule.equals("allow")) {
      this.path_rules.add(new PathRule(HostDirectives.ALLOWED, value));
    } else if (rule.equals("disallow")) {
      this.path_rules.add(new PathRule(HostDirectives.DISALLOWED, value));
    } else {
      logger.error("Invalid key in robots.txt passed to UserAgentRules: {}", rule);
    }
  }
  
  /**
   * Return the configured crawl delay in seconds
   * 
   * @return The configured crawl delay, or null if none was specified
   */
  public Double getCrawlDelay() {
    return crawl_delay;
  }
  
  /**
   * Return the specified preferred host name in robots.txt.
   * 
   * @return The specified hostname, or null if it was not specified
   */
  public String getPreferredHost() {
    return preferred_host;
  }
  
  /**
   * Return the listed sitemaps, or null if none was specified
   * 
   * @return
   */
  public List<String> getSitemap() {
    return sitemap;
  }
}
