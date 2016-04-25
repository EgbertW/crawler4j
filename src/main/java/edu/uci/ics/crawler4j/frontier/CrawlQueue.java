package edu.uci.ics.crawler4j.frontier;

import java.util.Collection;
import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.crawler.exceptions.QueueException;
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
     * @return True if the URL was added, false if it was already on the list or rejected
     */
    public boolean enqueue(WebURL url);
    
    /**
     * Append a list of new URLs to the queue.
     * 
     * @param urls The URLs to enqueue
     * 
     * @return A list of rejected URLS. Reasons for rejection can be things
     * such as duplicate URLs, invalid URLs, maximum queue size reached, etc.
     * In order to get more detail, getLastError can be queried or enqueue() can
     * be called separately.
     */
    public List<WebURL> enqueue(Collection<WebURL> urls);
    
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
     * @throws QueueException When Crawler already has an assigned page
     */
    public WebURL getNextURL(WebCrawler crawler, PageFetcher fetcher) throws QueueException;
    
    /**
     * Indicate that the specified crawler will no longer
     * crawl the specified URL.
     * 
     * @param crawler The crawler that abandons the request
     * @param url The URL that is abandoned
     * @throws QueueException When url was not assigned to crawler
     */
    public void abandon(WebCrawler crawler, WebURL url) throws QueueException;
    
    /**
     * Indicate that the specified crawler has completed retrieving
     * the specified URL.
     * 
     * @param crawler The Crawler that fetched the URL
     * @param url The URL that has been retrieved
     * @throws QueueException When url was not assigned to crawler
     */
    public void setFinishedURL(WebCrawler crawler, WebURL url) throws QueueException;
    
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
     * Return the amount of URLs in the queue or in progress for a specific seed offspring
     * 
     * @param seed_doc_id The seed doc id for which to return the number of offspring
     * @return The number of offspring for the seed
     */
    public long getNumOffspring(long seed_doc_id);
    
    /**
     * Remove all offspring of the given seed. This does not
     * guarantee that no URLs will be returned with this URL however;
     * if a assigned URL is abandoned, it will be returned to the queue,
     * and may be returned at a later moment. This should be limited
     * to the amount of crawlers there are.
     * 
     * @param seed_doc_id The docid of the seed for which to remove the offspring
     */
    public void removeOffspring(long seed_doc_id);

    /**
     * Reassign a URL from an old thread to a new thread
     * 
     * @param oldthread The thread whose URL needs to be reassigned
     * @param newthread The new thread that the URL will be assigned to
     * @return The reassigned URL, null if none was assigned.
     */
    public WebURL reassign(Thread oldthread, Thread newthread);
}