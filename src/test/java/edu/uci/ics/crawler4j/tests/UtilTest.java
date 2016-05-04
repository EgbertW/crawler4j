package edu.uci.ics.crawler4j.tests;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.crawler4j.util.Util;

public class UtilTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  private void checkLong(long l) {
    byte buf[] = Util.long2ByteArray(l);
    byte buf2[] = new byte[16];
    Util.putLongInByteArray(l, buf2, 4);
    long reproduced = Util.byteArray2Long(buf);
    assertEquals(l, reproduced);
    
    long reproduced2 = Util.extractLongFromByteArray(buf2, 4);
    assertEquals(l, reproduced2);
  }
  
  private void checkInt(int i) {
    byte buf[] = Util.int2ByteArray(i);
    byte buf2[] = new byte[16];
    Util.putIntInByteArray(i, buf2, 4);
    long reproduced = Util.byteArray2Int(buf);
    assertEquals(i, reproduced);
    
    long reproduced2 = Util.extractIntFromByteArray(buf2, 4);
    assertEquals(i, reproduced2);
  }
  
  @Test
  public void testLong() {
    checkLong(Long.MIN_VALUE);
    checkLong(Long.MIN_VALUE >> 2);
    checkLong(Long.MIN_VALUE >> 4);
    checkLong(Long.MIN_VALUE >> 6);
    checkLong(Long.MIN_VALUE << 2);
    checkLong(Long.MIN_VALUE << 4);
    checkLong(Long.MIN_VALUE << 6);
    
    checkLong(Long.MAX_VALUE);
    checkLong(Long.MAX_VALUE << 2);
    checkLong(Long.MAX_VALUE << 4);
    checkLong(Long.MAX_VALUE << 6);
    checkLong(Long.MAX_VALUE >> 2);
    checkLong(Long.MAX_VALUE >> 4);
    checkLong(Long.MAX_VALUE >> 6);
    
    for (int shift = 0; shift <= 8; ++shift) {
      long l = 1 << shift;
      checkLong(l);
    }
  }

  @Test
  public void testInt() {
    checkInt(Integer.MIN_VALUE);
    checkInt(Integer.MIN_VALUE << 2);
    checkInt(Integer.MIN_VALUE >> 2);
    
    checkInt(Integer.MAX_VALUE);
    checkInt(Integer.MAX_VALUE << 2);
    checkInt(Integer.MAX_VALUE >> 2);
    
    for (int shift = 0; shift <= 4; ++shift) {
      int i = 1 << shift;
      checkInt(i);
    }
  }
}
