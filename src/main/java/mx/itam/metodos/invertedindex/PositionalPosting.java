package mx.itam.metodos.invertedindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

public class PositionalPosting implements Writable, Comparable<PositionalPosting> {
    private Text id;
    private IntArrayWritable pos;
    
    public PositionalPosting(Text id, List<IntWritable> pos) {
      this.id = id;
      this.pos = new IntArrayWritable(pos.toArray(new IntWritable[0]));
    }

    public PositionalPosting() {
      this(new Text(), Lists.<IntWritable>newArrayList());
    }
    
    public PositionalPosting(PositionalPosting old) {
      this(new Text(old.id.toString()), 
              old.pos.asList());
    }

    public void write(DataOutput out) throws IOException {
      id.write(out);
      pos.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      pos.readFields(in);
    }

    public Text getId() {
      return id;
    }
    
    public int getTf() {
      return pos.get().length;
    }
    
    @Override
    public String toString() {
      return id + ":" + pos;
    }

    @Override
    public int compareTo(PositionalPosting other) {
      return id.compareTo(other.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PositionalPosting)) {
        return false;
      }
      PositionalPosting pp = (PositionalPosting) o;
      return id.equals(pp.id);
    }
    
  }
