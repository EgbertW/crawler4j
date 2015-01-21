package edu.uci.ics.crawler4j.frontier;

public class URLSeenBefore extends RuntimeException {
  private static final long serialVersionUID = -6635675249988125555L;
    
  private int docid;
      
  public URLSeenBefore(int docid) {
    super();
    this.docid = docid;
  }
      
  public int getDocid() {
    return docid;
  }
}