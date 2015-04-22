package knn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayes {
	public static Hashtable<String, Integer> table 
	= new Hashtable<String, Integer>(); 
	public static ArrayList<String> totalclass = new ArrayList<String>();
	
	public static class NaiveMapper extends 
	Mapper<Object, Text, Text, IntWritable>{
		public static IntWritable one = new IntWritable(1);
		@Override 
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			String [] line = value.toString().split(",");
			int num_items = line.length;
			
			//class statistic
			context.write(new Text(line[num_items-1]), one);
			
			String newkey = new String();
			for(int i=0; i<num_items-1; i++){
				newkey = line[num_items-1]+","+String.valueOf(i)
						+","+line[i];
				context.write(new Text(newkey), one);
			}
		}
	}

	public static class NaiveReduce extends
	Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable val: values){
				sum+=val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class InitialMapper extends
	Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			String [] line = value.toString().split("\t");
			table.put(line[0],Integer.parseInt(line[1]));
			String [] items = line[0].split(",");
			if (items.length==1){
				totalclass.add(line[0]);
			}
		}
	}

	//Input the train data
	public static class predictMapper extends
	Mapper<IntWritable, Text, IntWritable, Text>{
		@Override
		protected void map(IntWritable key, Text value, Context context)
		throws IOException, InterruptedException{
			double prob = 1.0;
			String[] line = value.toString().split(",");
			String partitionClass = new String();
			int num_items = line.length;
			for (String item: totalclass){
				double tempprob = table.get(item);
				for (int i=0; i<num_items-1; i++){
					 tempprob*=table.get(item+","+String.valueOf(i)+","+line[i]);
				}
				if (prob<tempprob){
					prob = tempprob;
					partitionClass = item;
				}
			}
			context.write(key, new Text(partitionClass));
		}
	}

	public void main(String[] args)throws Exception{
		  Configuration conf = new Configuration();
			args = new String[2];
			args[0]=new String("hdfs://192.168.75.128:9000/user/root/trainData");
			args[1]=new String("hdfs://192.168.75.128:9000/user/root/FreqCount");
		    Job job = new Job(conf, "Frequence Count");  
		    job.setJarByClass(KNN.class);  
		    job.setMapperClass(NaiveMapper.class);
		    job.setReducerClass(NaiveReduce.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(Text.class);  
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));  
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));    
		    job.waitForCompletion(true);
		    System.out.println("Frequence Count Ok");
		    
		    Configuration initconf = new Configuration();
			String[] initargs = new String[2];
			initargs[0]=new String("hdfs://192.168.75.128:9000/user/root/FreqCount");
			initargs[1]=new String("hdfs://192.168.75.128:9000/user/root/tempResult");
		    Job initjob = new Job(initconf, "NavieBayes Initiate");  
		    job.setJarByClass(KNN.class);  
		    job.setMapperClass(InitialMapper.class);
		    job.setOutputKeyClass(Text.class);  
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(initjob, new Path(initargs[0]));  
		    FileOutputFormat.setOutputPath(initjob,new Path(initargs[1]));    
		    initjob.waitForCompletion(true);
		    System.out.println("Initial Ok");
		    
		    Configuration predconf = new Configuration();
			String [] predargs = new String[2];
			predargs[0]=new String("hdfs://192.168.75.128:9000/user/root/testData");
			predargs[1]=new String("hdfs://192.168.75.128:9000/user/root/Result");
		    Job predjob = new Job(predconf, "Predict Reuslt");  
		    predjob.setJarByClass(KNN.class);  
		    predjob.setMapperClass(predictMapper.class);
		    predjob.setOutputKeyClass(IntWritable.class);  
		    predjob.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(predjob, new Path(predargs[0]));  
		    FileOutputFormat.setOutputPath(predjob,new Path(predargs[1]));    
		    predjob.waitForCompletion(true);
		    System.out.println("Predict Ok");
	}
}
