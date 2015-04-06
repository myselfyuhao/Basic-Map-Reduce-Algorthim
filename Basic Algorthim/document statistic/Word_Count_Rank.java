package wordcount; 
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

 
 public class Wordcount {
     public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
         private final static IntWritable one = new IntWritable(1);
         private Text word = new Text();
         @Override
         public void map(LongWritable key, Text value, Context context)
                 throws IOException, InterruptedException {
             StringTokenizer itr = new StringTokenizer(value.toString());
             while (itr.hasMoreTokens()) {
                 word.set(itr.nextToken());
                 context.write(word, one);
             }
         }
     }
 
     public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
         private IntWritable result = new IntWritable();
         @Override
         public void reduce(Text key, Iterable<IntWritable> values, Context context)
                 throws IOException, InterruptedException {
             int sum = 0;
             for (IntWritable val : values) {
                 sum += val.get();
             }
             result.set(sum);
             context.write(key, result);
         }
     }
     
     public static class InverseSortComparator
     extends IntWritable.Comparator{
    	 public int compare(WritableComparable a, WritableComparable b){
    		 return -super.compare(a,b);
    	 }
    	 public int compare(byte[] b1, int s1, int l1, 
    			 byte[] b2, int s2, int l2){
    				 return -super.compare(b1, s1, l1, b2, s2, l2);
    			 }
     }
     
     
 
     public static void main(String[] args) throws Exception {
         Configuration conf = new Configuration();
         String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
         if (otherArgs.length != 2) {
        	 System.err.println("Usage: MartrixMultiplication <in> <out>");
        	 System.exit(2);
         }
         Path tempDir = new Path("wordcount-temp-"+
         Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); 
         Job job = new Job(conf, "word count");
         job.setJarByClass(Wordcount.class);
         job.setMapperClass(TokenizerMapper.class);
         job.setCombinerClass(IntSumReducer.class);
         job.setReducerClass(IntSumReducer.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(IntWritable.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);
 
         FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
         FileOutputFormat.setOutputPath(job, tempDir);
         job.setOutputFormatClass(SequenceFileOutputFormat.class);
 
         if(job.waitForCompletion(true)){
        	 Job sortJob = new Job(conf, "Inverse Sort");
             sortJob.setJarByClass(Wordcount.class);
             
             FileInputFormat.addInputPath(sortJob, tempDir);
             sortJob.setInputFormatClass(SequenceFileInputFormat.class);  
             sortJob.setMapperClass(InverseMapper.class);
             sortJob.setNumReduceTasks(1);
             FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));
             sortJob.setOutputKeyClass(IntWritable.class);
             sortJob.setOutputValueClass(Text.class);
             sortJob.setSortComparatorClass(InverseSortComparator.class);
             System.exit(sortJob.waitForCompletion(true)?0:1);
         }
         FileSystem.get(conf).deleteOnExit(tempDir);     
     }
 }

 
 
 
 
 
