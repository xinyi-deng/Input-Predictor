import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Language {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text phrase = new Text();
    private Text word_count = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tmp = line.split("\t");
      String[] words = tmp[0].split(" ");
      if(words.length==1) return;
      if(Integer.parseInt(tmp[1])<=2) return;
      StringBuffer output1 = new StringBuffer();
      for(int i=0;i<words.length-1;i++){
    	  if(i!=0) output1.append(" ");
    	  output1.append(words[i]);
      }
      phrase.set(output1.toString());
      StringBuffer output2 = new StringBuffer();
      output2.append(words[words.length-1]);
      output2.append(" ");
      output2.append(tmp[1]);
      word_count.set(output2.toString());
      context.write(phrase,word_count);
      
    }
  }

  public static class IntSumReducer
       extends TableReducer<Text,Text,ImmutableBytesWritable> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	PriorityQueue<Gram> gram_que = new PriorityQueue<Gram>();
    	int totalCount = 0;
    	for (Text val : values) {
    		String[] tmp = val.toString().split(" ");
    		Gram newGram = new Gram(tmp[0],Integer.parseInt(tmp[1]));
    		totalCount+=newGram.count;
    		gram_que.add(newGram);
    		if(gram_que.size()>5) gram_que.poll();
    	}

    	Put put = new Put(Bytes.toBytes(key.toString()));
    	
    	while(gram_que.size()>0){
    		Gram output = gram_que.poll();
    		double prob = (double)output.count/totalCount;
    		put.add(Bytes.toBytes("data"),Bytes.toBytes(output.gram),Bytes.toBytes(String.valueOf(prob)));	
    	}
    	context.write(null, put);

    }
  }
  
  public static class Gram implements Comparable<Gram>{
	  String gram;
	  int count;
	  
	  public Gram(String gram,int count){
		  this.gram = gram;
		  this.count = count;
	  }
	  
	  public int compareTo(Gram other){
		  if(this.count!=other.count) return this.count-other.count;
		  else return this.gram.compareTo(other.gram);
	  }
	  
  }

  public static void main(String[] args) throws Exception {
	  
    Configuration conf = HBaseConfiguration.create();
    Job job = Job.getInstance(conf, "language");
    job.setJarByClass(Language.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    TableMapReduceUtil.initTableReducerJob("language",IntSumReducer.class,job);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}