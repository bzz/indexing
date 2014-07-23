package mx.itam.metodos.invertedindex;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.LineReader;

public class IndexCLI {

  private enum Op {AND, OR}
  
  public static void main(String[] args) throws Exception {
    Map<String, Writable[]> dictionary = loadIndex(args[0]);

    LineReader lr = new LineReader(new InputStreamReader(System.in));
    String line = null;
    System.out.print(">");
    while ((line = lr.readLine()) != null) {
      Op operator = Op.AND;
      if (line.startsWith("OR")) {
        operator = Op.OR;
      }
      List<String> tokens = analyze(line);
      LinkedList<Set<PositionalPosting>> candidates = Lists.newLinkedList();
      for (String token : tokens) {
        Writable[] postings = dictionary.get(token);
        if (postings != null) {
          Set<PositionalPosting> set = Sets.newHashSet();
          for (Writable w : postings) {
            set.add((PositionalPosting) w);
          }
          candidates.add(set);
        }
      }

      if (candidates.size() > 0) {
        Collections.sort(candidates, new Comparator<Set<PositionalPosting>>() {
          @Override public int compare(Set<PositionalPosting> arg0, Set<PositionalPosting> arg1) {
            return arg0.size() - arg1.size();
          }
        });
        Set<PositionalPosting> result = candidates.removeFirst();
        for (Set<PositionalPosting> pp : candidates) {
          if (operator == Op.AND) {
            result = Sets.intersection(result, pp);
          } else {
            result = Sets.union(result, pp);
          }
        }
        print(result);
      } else {
        System.out.println();
      }
      System.out.print(">");
    }
  }
  
  private static void print(Set<PositionalPosting> results) {
    for (PositionalPosting p : results) {
      System.out.println(p.getId());
    }
  }

  private static Map<String, Writable[]> loadIndex(String path) throws IOException {
    Map<String, Writable[]> dictionary = Maps.newHashMap();
    SequenceFile.Reader reader = null;
    try {
      Path out = new Path(path);
      Configuration conf = new Configuration();
      FileSystem fs = out.getFileSystem(conf);
      reader = new SequenceFile.Reader(fs, out, conf);
      Text text = new Text();
      PositionalPostingArrayWritable posting = new PositionalPostingArrayWritable();
      while (reader.next(text, posting)) {
        dictionary.put(text.toString(), posting.get());
      }
    } finally {
      if (reader != null) { reader.close(); };
    }
    return dictionary;
  }

  // private static PositionalPosting[] readPostings() {
  //
  // }

  private static List<String> analyze(String text) throws IOException, InterruptedException {
    List<String> result = Lists.newArrayList();
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);
    TokenStream ts = null;
    try {
      ts = analyzer.tokenStream("text", new StringReader(text));
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        String token = termAtt.toString();
        result.add(token);
      }
      ts.end();
    } finally {
      analyzer.close();
      if (ts != null) { ts.close(); }
    }
    return result;
  }

}
