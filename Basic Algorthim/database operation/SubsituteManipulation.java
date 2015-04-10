package databaseOperation;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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

public class Subsitute {
	
	public class SubsituteMapper 
	extends Mapper<Object, Text, Text, Text>{
		String file = new String();
		@Override
		protected void setup(Context context){
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			file = new String(filename);
		}
		
		
		
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			if (file.equals("R.txt")){
				context.write(value, new Text("R"));
			}else if(file.equals("T.txt")){
				context.write(value, new Text("T"));
			}
		}
	}
	
	public class SubsituteReducer 
	extends Reducer<Text, Text, Text, NullWritable>{
		int fileRecord[] = new int[2];
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			fileRecord[0]=0;
			fileRecord[1]=0;
			for (Text val: values){
				if(val.toString().equals("R")){
					fileRecord[0]+=1;
				}else if(val.toString().equals("T")){
					fileRecord[1]+=1;
				}
			}
			if(fileRecord[0]==1 && fileRecord[1]==0)context.write(key, NullWritable.get());			
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
	    Job job = new Job(conf, "Subsitute");  
	    job.setJarByClass(Subsitute.class);  
	    job.setMapperClass(SubsituteMapper.class);
	    job.setReducerClass(SubsituteReducer.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(NullWritable.class);  
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    System.out.println("ok");  
	    }  
		
}

