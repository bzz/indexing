package invertedindex.mr.writable;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class PositionalPostingArrayWritable extends ArrayWritable {
  public PositionalPostingArrayWritable() { 
    super(PositionalPosting.class); 
  } 
  public PositionalPostingArrayWritable(PositionalPosting[] values) { 
    super(PositionalPosting.class, values); 
  } 
  
  public String toString() {  
    return Arrays.asList(toStrings()).toString();
  }
}

