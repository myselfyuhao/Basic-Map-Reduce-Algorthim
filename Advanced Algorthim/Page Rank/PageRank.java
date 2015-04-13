package pagerank;
import java.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyPageRank {
	public static class PageRankMapper
	extends Mapper<Object, Text, Text, Text>{
		String URL = new String();
		double PR;
		double donatePR;
		
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			String[] line = value.toString().split("\t");
			URL = line[0];
			PR = Double.parseDouble(line[1]);
			String[] linklist = line[2].split(","); 
			context.write(new Text(URL),new Text(line[2]
					+"\tLL"));
			donatePR = PR*1.0/linklist.length;
			for (String link: linklist){
				context.write(new Text(link), 
						new Text(String.valueOf(donatePR)+"\tPR"));
			}	
		}
	}
	
	public static class PageRankReducer
extends Reducer<Text, Text, Text, Text>{
	double damping=0.85;
	String URL = new String();
	String linklist = new String();
	double finalPR;
	int linklistLen;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException{
		double sumPR=0.0;
		for (Text val: values){
			String[] line = val.toString().split("\t");
			if (line[1].equals("LL")){
				linklist = line[0];
			}
			else{
				sumPR+=Double.parseDouble(line[0]);
			}
		}
		linklistLen = linklist.split(",").length;
		finalPR = sumPR*damping+(1-damping)*1.0/linklistLen;
		context.write(key, new Text(String.valueOf(finalPR)+
				"\t"+linklist));
		}		
	}

    public static class ViewMapper 
    extends Mapper<Object, Text, Text, Text>{
    	@Override
    	protected void map(Object key, Text value, Context context)
    	throws IOException, InterruptedException{
    		String[] line = value.toString().split("\t");
    		context.write(new Text(line[1]), new Text(line[0]));
    	}
    }

	public static void main(String args[]) throws Exception {
		int loop;
		int iternum = 15;
		for(loop= 0; loop<iternum; loop++){
			Configuration conf = new Configuration();
			args = new String[2];
			args[0]=new String("hdfs://192.168.164.128:9000/user/root/in"+
					String.valueOf(loop));
			args[1]=new String("hdfs://192.168.164.128:9000/user/root/in"+
			String.valueOf(loop+1));
		    Job job = new Job(conf, "PageRank");  
		    job.setJarByClass(MyPageRank.class);  
		    job.setMapperClass(PageRankMapper.class);
		    job.setReducerClass(PageRankReducer.class);
		    job.setOutputKeyClass(Text.class);  
		    job.setOutputValueClass(Text.class);  
		    FileInputFormat.addInputPath(job, new Path(args[0]));  
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));    
		    job.waitForCompletion(true);
		    System.out.println(String.valueOf(loop+1)+" Time");
		}
		
		
		Configuration newconf = new Configuration();
		String[] newargs = new String[2];
		newargs[0]=new String("hdfs://192.168.164.128:9000/user/root/in"+
				String.valueOf(loop));
		newargs[1]=new String("hdfs://192.168.164.128:9000/user/root/Result");
	    Job sortjob = new Job(newconf, "PageRank");  
	    sortjob.setJarByClass(MyPageRank.class);  
	    sortjob.setMapperClass(ViewMapper.class);
	    sortjob.setOutputKeyClass(Text.class);  
	    sortjob.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(sortjob, new Path(newargs[0]));  
	    FileOutputFormat.setOutputPath(sortjob,new Path(newargs[1]));    
	    sortjob.waitForCompletion(true);
	    System.out.println("Ok");
    }  
}
