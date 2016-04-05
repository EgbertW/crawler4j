package edu.uci.ics.crawler4j.frontier;

import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.url.WebURL;

public interface CrawlQueue {
    /**
     * Set the crawl configuration to be used for managing the queue.
     * 
     * @param config The configuration to use
     */
    public void setCrawlConfiguration(CrawlConfig config);
    
    /**
     * Append a new URL to the queue.
     * 
     * @param url The URL to enqueue
     */
    public void enqueue(WebURL url);
    
    /**
     * Append a new URL to the queue.
     * 
     * @param url The URL to enqueue
     * 
     * @return A list of rejected URLS. Reasons for rejection can be things
     * such as duplicate URLs, invalid URLs, maximum queue size reached, etc.
     * In order to get more detail, getLastError can be queried or enqueue() can
     * be called separately.
     */
    public List<WebURL> enqueue(List<WebURL> urls);
    
    /**
     * Return the last error string while performing any action on the queue.
     * 
     * @return The latest error that occured.
     */
    public String getLastError();
    
    /**
     * Get the next URL for the specified WebCrawler
     *  
     * @param crawler The crawler that will perform the request
     * @param fetcher The page fetcher that can be queried for 
     *                last retrieve times in order to find the best
     *                URL respecting the politeness delay.
     * @return The next URL to crawl
     */
    public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher);
    
    /**
     * Indicate that the specified crawler will no longer
     * crawl the specified URL.
     * 
     * @param crawler The crawler that abandons the request
     * @param url The URL that is abandoned
     */
    public void abandon(WebCrawler crawler, WebURL url);
    
    /**
     * Indicate that the specified crawler has completed retrieving
     * the specified URL.
     * 
     * @param crawler The Crawler that fetched the URL
     * @param url The URL that has been retrieved
     */
    public void setFinishedURL(WebCrawler crawler, WebURL url);
    
    /** 
     * Return the amount of URLs in the queue 
     * 
     * @return The amount of URLs in the queue
     */
    public long getQueueSize();
    
    /**
     * Return the number of URLs currently in progress
     * 
     * @return The amount of URLs that have been claimed by a crawler
     */
    public long getNumInProgress();
    
    /**
     * Return the amount of URLs in the queue for a specific seed offspring
     * 
     * @param seed_doc_id
     */
    public long getNumOffspring(long seed_doc_id);
    
    /**
     * Declare the seed as finished -> no more URLs from this seed will
     * be handed out. However, URLs that have been handed out before will
     * most likely still be crawled.
     * 
     * @param seed_doc_id
     */
    public void setSeedFinished(long seed_doc_id);
}