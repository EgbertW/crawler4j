package edu.uci.ics.crawler4j.parser;

import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.crawler4j.url.WebURL;

public class XMLParseData implements ParseData {
  
  private Set<WebURL> outgoingUrls = new HashSet<>();
  private String XMLContent;
  
  
  public String getXMLContent() {
    return XMLContent;
  }

  public void setXMLContent(String textContent) {
    this.XMLContent = textContent;
  }
  
  @Override
  public Set<WebURL> getOutgoingUrls() {

    return outgoingUrls;
  }

  @Override
  public void setOutgoingUrls(Set<WebURL> outgoingUrls) {
    this.outgoingUrls = outgoingUrls; 
  }
}
