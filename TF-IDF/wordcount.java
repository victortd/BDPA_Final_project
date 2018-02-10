package cs.bigdata.Lab2;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcount extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		      int wordcountresult = ToolRunner.run(new Configuration(), new wordcount(), args);
		      
		      System.exit(wordcountresult);
		   }
	
	
	  public static class WordCountMapper
      extends Mapper<Object, Text, Text, IntWritable>{

   private final static IntWritable one = new IntWritable(1);
   private Text word = new Text();

   public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
	 
		 String filename = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
		 
	 String line = value.toString().toLowerCase();
     StringTokenizer itr = new StringTokenizer(line, 
                                               " \t\n\r&\\-_!.;,()\"\'/:+=$[]§?#*|{}~<>@`");
     while (itr.hasMoreTokens()) {
       word.set(itr.nextToken());
       String wordindoc = word.toString() + ";" + filename;
       context.write(new Text(wordindoc), one);
     }
   }
 }

  public static class WordCountReducer
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

  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    Job job = Job.getInstance(conf, "wordcount");
    job.setNumReduceTasks(1);
    job.setJarByClass(wordcount.class);
    
    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountReducer.class);
    job.setReducerClass(WordCountReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("wordcountoutput"));
    FileSystem filesys = FileSystem.get(getConf());
	  if (filesys.exists(new Path("wordcountoutput")))
		  filesys.delete(new Path("wordcountoutput"), true);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    return 0;
  }
}

