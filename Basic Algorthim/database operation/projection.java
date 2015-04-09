/*Select col[0], col[2] from ...*/
package filterColumn;

import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColumnFilter {
	public static class ProjectMapper
	extends Mapper<Object, Text, NullWritable, Text>{
		Text words = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			String [] line = value.toString().split(" ");
			words = new Text(line[0]+":"+line[2]);
			context.write(NullWritable.get(), words);
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
	        Job job = new Job(conf, "projection");  
	         job.setJarByClass(ColumnFilter.class);  
	        job.setMapperClass(ProjectMapper.class);   
	        job.setOutputKeyClass(NullWritable.class);  
	        job.setOutputValueClass(Text.class);  
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	        System.exit(job.waitForCompletion(true) ? 0 : 1);  
	        System.out.println("ok");  
	    }  
}

