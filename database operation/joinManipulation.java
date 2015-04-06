package joinManipulation;
import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinManipulation {
	public static class DataJoinMapper
	extends Mapper<Object, Text, Text, Text>{
		private String file = new String();
		private String values = new String();
		
		@Override
		protected void setup(Context context){
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			file = new String(filename);
		}
		
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			String [] line = value.toString().split("\t");
			int itemnumber = line.length;
			
			if (file.equals("user.txt")){
				values = new String("U");
				for (int i=1; i<itemnumber;i++){
					values+="\t"+line[i];
				}
			}
			
			if (file.equals("log.txt")){
				values = new String("L");
				for (int i=1; i<itemnumber;i++){
					values+="\t"+line[i];
				}
			}
			context.write(new Text(line[0]),new Text(values));
		}
	}
	
public static class DataJoinReducer
extends Reducer<Text, Text, Text, Text>{
	
	private String left = new String();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException{
		//First scan to get the value
		for (Text val: values){
			String[] items = val.toString().split("\t");
			if(items[0].equals("U")){
				for (int i=1; i<items.length; i++){
					if (i==1)left=items[i];
					else left+="\t"+items[i];
				}
				break;
			}
		}
				
		for (Text val: values){
			String right = new String();
			String[] items = val.toString().split("\t");
			if(items[0].equals("L")){
				for (int i=1; i<items.length; i++){
					right+="\t"+items[i];
				}
			}
			context.write(key, new Text(left+right));
		}		
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
	    job.setJarByClass(JoinManipulation.class);  
	    job.setMapperClass(DataJoinMapper.class);
	    job.setReducerClass(DataJoinReducer.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    System.out.println("ok");  
	    }  
}






