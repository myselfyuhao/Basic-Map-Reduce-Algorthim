package matrixMultiply;

import java.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixAdd{
	
	public static class MatrixAddMapper
	extends Mapper<Object, Text, Text, Text>{
		private Text map_key = new Text();
		private Text map_value = new Text();
	    
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			String [] line = value.toString().split("\t");
			map_key.set(line[0]+"\t"+line[1]);
			map_value.set(line[2]);
			context.write(map_key, map_value);
		}
	}

	public static class MatrixAddReducer 
	extends Reducer<Text,Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException{
			int sum = 0;
			for (Text val: value){
				sum += Integer.parseInt(val.toString()); 
			}
			context.write(key, new Text(String.valueOf(sum)));
		}
		
	}

	public static void main(String[] args) throws Exception {  
		Configuration conf = new Configuration();  
	    String[] otherArgs = new GenericOptionsParser(conf, args)
	    .getRemainingArgs();  
	    if (otherArgs.length != 2) {  
	    	System.err.println("Usage: numbersum <in> <out>");  
	    	System.exit(2);  
	    	}  
	    Job job = new Job(conf, "number sum");  
	    job.setJarByClass(MatrixAdd.class);  
	    job.setMapperClass(MatrixAddMapper.class);
	    job.setReducerClass(MatrixAddReducer.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    System.out.println("ok");  
	    }  
}
