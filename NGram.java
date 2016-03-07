import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NGram {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      line = line.replaceAll("[^a-zA-Z]+", " ").toLowerCase().trim();
      if(line.length()==0) return;
      String[] gram = line.split(" ");
      for(int i=0;i<gram.length;i++){
    	  StringBuffer sb = new StringBuffer();
    	  for(int j=i;j<i+5 && j<gram.length;j++){
    		  if(j!=i) sb.append(" ");
    		  sb.append(gram[j]);
    		  word.set(sb.toString());
    	      context.write(word, one);	   		  
    	  }
      }
      
//      StringTokenizer itr = new StringTokenizer(value.toString());
//		ArrayList<String> ngram = new ArrayList<String>();
//      while (itr.hasMoreTokens()) {
//    	  String[] newWords = itr.nextToken().split("[^a-zA-Z]");
//    	  for(int i=0;i<newWords.length;i++){
//    		  if(newWords[i].length()==0) continue;
//    		  newWords[i]=newWords[i].toLowerCase();
//    		  if(ngram.size()==5){
//    			  ngram.remove(0);
//    		  }
//    		  ngram.add(newWords[i]);
//    		  for(int j=0;j<ngram.size();j++){
//    			  StringBuffer sb = new StringBuffer();
//	    		  for(int k=j;k<ngram.size();k++){	    			  
//	    			  if(k!=j) sb.append(" ");
//	    			  sb.append(ngram.get(k));
//	    		  }
//	    		  word.set(sb.toString());
//	    	      context.write(word, one);	    		  
//    		  }
//    	  }
//      }
    }
    

  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ngram");
    job.setJarByClass(NGram.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}