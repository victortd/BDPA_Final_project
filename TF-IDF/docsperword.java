
package cs.bigdata.Lab2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.ArrayList;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class docsperword extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      int docsperwordresult = ToolRunner.run(new Configuration(), new docsperword(), args);
      
      System.exit(docsperwordresult);
   }
   
   public static class docsperwordMapper extends Mapper<LongWritable, Text, Text, Text> {
    
      public void map(LongWritable key, Text val, Context context)
              throws IOException, InterruptedException {
    	  
		   String[] dpw = val.toString().split("\t|;");
		   
		   context.write(new Text(dpw[0]), new Text(dpw[1]+";"+dpw[2]+";"+dpw[3]));
      }
   }

   public static class docsperwordReducer extends Reducer<Text, Text, Text, DoubleWritable> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
       
  		String word = key.toString();
  		ArrayList<String> contents = new ArrayList<String>();
  		
  		double docsperword = 0;
  		
  		for (Text val : values) {
  			contents.add(val.toString());
  			docsperword = docsperword + (double) 1;
  		}
  		
  		for (int i = 0; i < contents.size(); i++) {
  			
  			String[] contentlist = contents.get(i).split("\t|;");
  			String wordandbook = word+";"+contentlist[0];
  			double wordcount = Integer.parseInt(contentlist[1]);
  			double wpd = Integer.parseInt(contentlist[2]);
  			double tfidf = (wordcount/wpd)*Math.log(2 / docsperword);
  			
  			context.write(new Text(wordandbook), new DoubleWritable(tfidf));    
           }
      }
  

  
   } 
          public int run(String[] args) throws Exception {
              Configuration conf = this.getConf();

              Job job = Job.getInstance(conf, "docsperword");
              job.setNumReduceTasks(1);
              job.setJarByClass(docsperword.class);
              job.setOutputKeyClass(Text.class);
              job.setOutputValueClass(Text.class);

              job.setMapperClass(docsperwordMapper.class);
              job.setReducerClass(docsperwordReducer.class);

              job.setInputFormatClass(TextInputFormat.class);
              job.setOutputFormatClass(TextOutputFormat.class);

              FileInputFormat.addInputPath(job, new Path("wordsperdocoutput"));
              FileOutputFormat.setOutputPath(job, new Path("docsperwordoutput"));
              FileSystem filesys = FileSystem.get(getConf());
         	  if (filesys.exists(new Path("docsperwordoutput")))
         		 filesys.delete(new Path("docsperwordoutput"), true);
         	 System.exit(job.waitForCompletion(true) ? 0 : 1);
              
              return 0;
           }
      
      }


