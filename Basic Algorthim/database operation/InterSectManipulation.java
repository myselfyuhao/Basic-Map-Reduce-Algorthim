package databaseOperation;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Intersect {
	Text one = new Text(String.valueOf(1));
	
	public class IntersectMapper 
	extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			context.write(value, one);
		}
	}
	
	public class IntersectReducer 
	extends Reducer<Text, Text, Text, NullWritable>{
		int sum=0;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			for (Text val: values){
				sum+=Integer.parseInt(val);
			}
			if(sum==2)context.write(key, NullWritable.get());
			
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
	    Job job = new Job(conf, "Intersect");  
	    job.setJarByClass(Intersect.class);  
	    job.setMapperClass(IntersectMapper.class);
	    job.setReducerClass(IntersectReducer.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(NullWritable.class);  
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    System.out.println("ok");  
	    }  
		
}

