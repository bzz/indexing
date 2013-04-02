package mx.itam.metodos.invertedindex;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class IndexMapper extends Mapper<LongWritable, Text, Text, PositionalPosting> {

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
          InterruptedException {
    String[] values = value.toString().split("\t");
    if (values.length > 1) {
      analyze(values[0], values[1], context);
    }
  }

  private void analyze(String id, String text, Context context) throws IOException,
          InterruptedException {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);
    TokenStream ts = analyzer.tokenStream("text", new StringReader(text));
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);
    Multimap<String, IntWritable> map = LinkedHashMultimap.create();
    Text key = new Text(id);
    try {
      ts.reset();
      while (ts.incrementToken()) {
        String token = termAtt.toString();
        map.put(token, new IntWritable(offsetAtt.startOffset()));
      }
      ts.end();
    } finally {
      ts.close();
    }
    for (Map.Entry<String, Collection<IntWritable>> me : map.asMap().entrySet()) {
      Text token = new Text(me.getKey());
      context.write(token, new PositionalPosting(key, Lists.newArrayList(me.getValue())));
    }
  }
}