
package cs.bigdata.Lab2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordsperdoc extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      int wordsperdocresult = ToolRunner.run(new Configuration(), new wordsperdoc(), args);
      
      System.exit(wordsperdocresult);
   }


   public static class wordsperdocMapper
   extends Mapper <LongWritable, Text, Text, Text>{

	   public void map(LongWritable key, Text val, Context context
                ) throws IOException, InterruptedException {
    
		   String[] wpd = val.toString().split("\t|;");
		   
    context.write(new Text(wpd[1]), new Text(wpd[0]+";"+wpd[2]));
  }
}

   public static class wordsperdocReducer
   extends Reducer<Text,Text,Text,Text> {

     public void reduce(Text key, Iterable<Text> values,
                        Context context
                        ) throws IOException, InterruptedException {
       
          int sum = 0;
          HashMap <String, String > counter = new HashMap <String, String >();
          
          for (Text val : values) {
              String [] input = val.toString().split("\t|;") ;
              int wordsperdoc = Integer.parseInt(input[1]);
              counter.put(input[0]+";"+key.toString(), ""+wordsperdoc);
              sum += wordsperdoc;
            }
      	  
      	  
      	    Set setCount = counter.entrySet();
            Iterator itr = setCount.iterator();
            
            while(itr.hasNext()) {
                Entry e = (Entry)itr.next();
                context.write(new Text(e.getKey().toString()), new Text(e.getValue()+";"+sum));  
                
           }
   
      }
   }
   
   public int run(String[] args) throws Exception {
	      
	      Configuration conf = this.getConf();
	      Job job = Job.getInstance(conf, "wordsperdoc");
	      
	      job.setNumReduceTasks(1);
	      job.setJarByClass(wordsperdoc.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);

	      job.setMapperClass(wordsperdocMapper.class);
	      job.setReducerClass(wordsperdocReducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("wordcountoutput"));
	      FileOutputFormat.setOutputPath(job, new Path("wordsperdocoutput"));
	      FileSystem filesys = FileSystem.get(getConf());
	 	  if (filesys.exists(new Path("wordsperdocoutput")))
	 		  filesys.delete(new Path("wordsperdocoutput"), true);
	      System.exit(job.waitForCompletion(true) ? 0 : 1);
	      
	      return 0;
	   }
}

