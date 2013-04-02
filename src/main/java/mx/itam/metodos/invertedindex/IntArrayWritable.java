package mx.itam.metodos.invertedindex;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

public class IntArrayWritable extends ArrayWritable {
  public IntArrayWritable() { 
    super(IntWritable.class); 
  } 
  public IntArrayWritable(IntWritable[] values) { 
    super(IntWritable.class, values); 
  } 
  
  public String toString() {  
    return Arrays.asList(toStrings()).toString();
  }
  
  public List<IntWritable> asList() {
    List<IntWritable> list = Lists.newArrayList();
    for (Writable w: get()) {
      list.add((IntWritable) w);
    }
    return list;
  }
}

