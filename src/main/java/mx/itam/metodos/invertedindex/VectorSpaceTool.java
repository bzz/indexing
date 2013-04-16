package mx.itam.metodos.invertedindex;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.LineReader;

public class VectorSpaceTool {

  public class Score implements Comparable<Score> {

    private String id;

    private int score;

    public Score(String id, int score) {
      this.id = id;
      this.score = score;
    }

    @Override
    public int compareTo(Score o) {
      return score - o.score;
    }
    
    public String toString() {
      return id + ":" + score;
    }

  }

  private Map<String, Writable[]> dictionary;

  private int N;

  private enum Op {
    AND, OR
  }

  public static void main(String[] args) throws Exception {
    new VectorSpaceTool(args[0]);
  }

  public VectorSpaceTool(String path) throws Exception {
    loadIndex(path);
    LineReader lr = new LineReader(new InputStreamReader(System.in));
    String line = null;
    System.out.print(">");
    while ((line = lr.readLine()) != null) {
      Op operator = Op.AND;
      if (line.startsWith("OR")) {
        operator = Op.OR;
      }
      List<String> tokens = analyze(line);
      Multimap<String, Integer> candidates = HashMultimap.create();
      for (String token : tokens) {
        Writable[] postings = dictionary.get(token);
        int idf = (N / postings.length);
        if (postings != null) {
          Set<PositionalPosting> set = Sets.newHashSet();
          for (Writable w : postings) {
            PositionalPosting pp = (PositionalPosting) w;
            int tf = pp.getTf();
            int tfidf = idf * tf;
            candidates.put(pp.getId().toString(), tfidf);
            set.add(pp);
          }
        }
      }
      if (candidates.size() > 0) {
        List<Score> result = Lists.newArrayList();
        for (Entry<String, Collection<Integer>> entry : candidates.asMap().entrySet()) {
          int sum = 0;
          for (Integer w : entry.getValue()) {
            sum += w.intValue();
          }
          result.add(new Score(entry.getKey(), sum));
        }
        Collections.sort(result);
        print(result);
      } else {
        System.out.println();
      }
      System.out.print(">");
    }
  }

  private static void print(Collection<Score> results) {
    for (Score p : results) {
      System.out.println(p);
    }
  }

  private void loadIndex(String path) throws IOException {
    Path out = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = out.getFileSystem(conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, out, conf);
    Text text = new Text();
    PositionalPostingArrayWritable posting = new PositionalPostingArrayWritable();
    this.dictionary = Maps.newHashMap();
    Set<String> count = Sets.newHashSet();
    while (reader.next(text, posting)) {
      dictionary.put(text.toString(), posting.get());
      for (Writable w : posting.get()) {
        PositionalPosting pp = (PositionalPosting) w;
        count.add(pp.getId().toString());
      }
    }
    this.N = count.size();
  }

  // private static PositionalPosting[] readPostings() {
  //
  // }

  private static List<String> analyze(String text) throws IOException, InterruptedException {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_41);
    TokenStream ts = analyzer.tokenStream("text", new StringReader(text));
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    List<String> list = Lists.newArrayList();
    try {
      ts.reset();
      while (ts.incrementToken()) {
        String token = termAtt.toString();
        list.add(token);
      }
      ts.end();
    } finally {
      ts.close();
    }
    return list;
  }

}
