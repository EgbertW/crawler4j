package edu.uci.ics.crawler4j.url;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is a singleton which obtains a list of TLDs (from online or a zip file) in order to compare against those TLDs
 * */
public class TLDList {

  private final static String TLD_NAMES_ONLINE_URL = "https://publicsuffix.org/list/effective_tld_names.dat";
  private final static String TLD_NAMES_ZIP_FILENAME = "tld-names.zip";
  private final static String TLD_NAMES_TXT_FILENAME = "tld-names.txt";
  private final static Logger logger = LoggerFactory.getLogger(TLDList.class);

  private static boolean online_update = false;
  private Set<String> tldSet = new HashSet<>(10000);

  private static TLDList instance = new TLDList(); // Singleton

  private TLDList() {
    if (online_update)
    {
      URL url;
      try {
        url = new URL(TLD_NAMES_ONLINE_URL);
      } catch (MalformedURLException e) {
        // This cannot happen... No need to treat it
        logger.error("Invalid URL: {}", TLD_NAMES_ONLINE_URL);
        throw new RuntimeException(e);
      }
      
      try (InputStream stream = url.openStream()) {
        logger.debug("Fetching the most updated TLD list online");
        readStream(stream);
        return;
      } catch (Exception ex) {
        logger.error("Couldn't fetch the online list of TLDs from: {}", TLD_NAMES_ONLINE_URL);
      }
    }
      
    logger.info("Fetching the list from a local file {}", TLD_NAMES_ZIP_FILENAME);
    String filename = this.getClass().getClassLoader().getResource(TLD_NAMES_ZIP_FILENAME).getFile();
    ZipFile zipFile = null;
    try {
      zipFile = new ZipFile(filename);
    } catch (IOException e) {
      logger.error("Couldn't read the TLD list from file");
      throw new RuntimeException(e);
    }
    
    ZipArchiveEntry entry = zipFile.getEntry(TLD_NAMES_TXT_FILENAME);
    try (InputStream stream = zipFile.getInputStream(entry)) {
      readStream(stream);
      return;
    } catch (IOException e) {
      logger.error("Couldn't read the TLD list from file");
      throw new RuntimeException(e);
    } finally {
      try {
        zipFile.close();
      } catch (IOException e) {
       // Nothing, at least we tried :-)
      }
    }
  }
 
  private void readStream(InputStream stream)
  {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("//")) {
          continue;
        }
        tldSet.add(line);
      }
    }
    catch (IOException e)
    {
      logger.warn("Error while reading TLD-list: {}", e.getMessage());
    }
  }

  public static TLDList getInstance() {
    return instance;
  }
  
  public static void setUseOnline(boolean online) {
    online_update = online;
  }

  public boolean contains(String str) {
    return tldSet.contains(str);
  }
}