package edu.uci.ics.crawler4j.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ByteSerializable {
  /**
   * @return The amount of bytes required to serialize the element
   */
  public int getSerializedLength();
  
  /** Serialize the object into a byte buffer.
   * 
   * @param output Where to write the output to
   * @return The amount of bytes written
   * @throws IOException When writing fails
   */
  public int serialize(DataOutput output) throws IOException;
  
  /** 
   * Read the object from the stream
   * 
   * @param input The character buffer from which to read 
   * @return The amount of bytes read
   * @throws IOException When reading fails
   */
  public int unserialize(DataInput input) throws IOException;
}
