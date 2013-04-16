package mx.itam.metodos.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Path data = new Path(args[0]);
    Path out = new Path(args[1]);
    out.getFileSystem(conf).delete(out, true);
    return buildIndex(data, out, conf) ? 0 : 1;
  }
  
	public boolean buildIndex(Path data, Path out, Configuration conf) throws Exception {
		Job job = new Job(conf, "invertedindex");
    System.err.println(IndexMapper.class);
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PositionalPosting.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PositionalPostingArrayWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, data);
		FileOutputFormat.setOutputPath(job, out);
		return job.waitForCompletion(true);
	}

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
    System.exit(res);
  }
}