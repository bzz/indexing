package invertedindex.mr.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Posting implements Writable {
    private Text document;
    private IntWritable pos;
    
    public Posting(Text document,IntWritable pos) {
      this.document = document;
      this.pos = pos;
    }

    public Posting() {
      this(new Text(), new IntWritable());
    }
    
    public Posting(Posting old) {
      this(new Text(old.document.toString()), 
          new IntWritable(old.pos.get()));
    }

    public void write(DataOutput out) throws IOException {
      document.write(out);
      pos.write(out);
    }

    public void readFields(DataInput in) throws IOException {
      document.readFields(in);
      pos.readFields(in);
    }

    public String toString() {
      return document + ":" + pos;
    }
    
    public String getDocument() {
      return document.toString();
    }
    
    public int getPosition() {
      return pos.get();
    }
  }
