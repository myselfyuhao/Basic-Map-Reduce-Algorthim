package findfile;
import java.io.*;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Filestatistic {
	static int inloop = 0;
	static int outloop = 1;
	public static class fileMap extends Mapper<Object, Text, Text, Text>{
		private Text documentId;
		private Text word = new Text();
		
		@Override
		protected void setup(Context context){
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			documentId = new Text(filename);
		}
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			String[] words = value.toString().split(" ");
			for (String val: words){
				word.set(val);
				context.write(word, documentId);
			}
		}
	}
	
	public static class fileReducer extends Reducer<Text, Text, Text, Text>{
		private Text docIds = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			HashSet<Text> uniqueDocIds = new HashSet<Text>();
			for (Text val: values){
				uniqueDocIds.add(val);
			}
			docIds.set(new Text(StringUtils.join(uniqueDocIds,",")));
			context.write(key, docIds);
		}
	}

	public static void main(String args[])
			throws Exception{
		  Configuration conf = new Configuration();  
	       String[] otherArgs = new GenericOptionsParser(conf, args)  
	               .getRemainingArgs();  
	       if (otherArgs.length != 2) {  
	           System.err.println("Usage: numbersum <in> <out>");  
	           System.exit(2);  
	       }  
	       Job job = new Job(conf, "reverse Index");  
	       job.setJarByClass(Filestatistic.class);  
	       job.setMapperClass(fileMap.class);  
	       job.setReducerClass(fileReducer.class);
	       job.setMapOutputKeyClass(Text.class);
	       job.setOutputValueClass(Text.class);
	       job.setOutputKeyClass(Text.class);  
	       job.setOutputValueClass(Text.class);
	       	       
	       Path inputPath = new Path(otherArgs[0]);
	       Path outputPath = new Path(otherArgs[1]);
	       FileInputFormat.addInputPath(job, inputPath);  
	       FileOutputFormat.setOutputPath(job,outputPath); 
	       Path deletePath = new Path(otherArgs[1]+"/_SUCCESS");
	       outputPath.getFileSystem(conf).delete(deletePath,true);
	       System.exit(job.waitForCompletion(true) ? 0 : 1);  
	       System.out.println("ok");  
		
	}
	

}
