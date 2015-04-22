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


public class KNN {
	public static ArrayList<ArrayList<Double>> trainData = new ArrayList<ArrayList<Double>>();
	public static int numberOfNeighbour = 5;
	
	public static class InitialTrainDataMapper
	extends Mapper<Object, Text, Text, Text>{
		int k = 0;
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			ArrayList<Double> list = new ArrayList<Double>();
			String[] line = value.toString().split(",");
			for (String item: line){
				list.add(Double.parseDouble(item));
			}
			trainData.add(list);
		}	
	}

	public static class KNNMapper extends 
	Mapper<IntWritable, Text, IntWritable, Text>{
		
		protected static double Cosdistance(ArrayList<Double> pointA, ArrayList<Double> pointB){
			double distance;
			double powerA = 0.0;
			double powerB = 0.0;
			double powerAB = 0.0;
			for (int i=0; i<pointA.size()-1;i++){
				powerA += pointA.get(i)*pointA.get(i);
				powerB += pointB.get(i)*pointB.get(i);
				powerAB += pointA.get(i)*pointB.get(i);
			}
			distance = (double)(1-powerAB/(Math.sqrt(powerA)*Math.sqrt(powerB)));
			return distance;
		}
		
		protected static int findMax(ArrayList<Double> list){
			double maxValue = 0;
			int index = -1;
			for (int i=0; i<list.size();i++){
				if (maxValue<list.get(i)){
					maxValue = list.get(i);
					index = i;
				}
			}
			return index;
		}
		
		@Override
		protected void map(IntWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			ArrayList<Double> list = new ArrayList<Double>();
			String[] line = value.toString().split(" ");
			for (String item: line){
				list.add(Double.parseDouble(item));
			}
			
			ArrayList<Double>  distances = new ArrayList<Double>();
			ArrayList<Integer> trainLabel = new ArrayList<Integer>();
			for (int i=0; i<trainData.size(); i++){
				if(i<numberOfNeighbour){
					distances.add(Cosdistance(list, trainData.get(i)));
					trainLabel.add(i);
				}
				else{
					int index = findMax(distances);
					if (Cosdistance(list, trainData.get(i))<distances.get(index)){
						distances.remove(distances.get(index));
						trainLabel.remove(trainLabel.get(index));
						distances.add(Cosdistance(list, trainData.get(i)));
						trainLabel.add(i);
					}
				}
			}
			
			for (int j=0; j<numberOfNeighbour; j++){
				double partitionclass = trainData.get(trainLabel.get(j)).get(line.length-1);
				context.write(key, new Text(String.valueOf(partitionclass)));
			}
		}
	}

	public static class KNNReducer extends 
	Reducer<IntWritable, Text, IntWritable, Text>{
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			Hashtable<Text, Integer> numbers = new Hashtable<Text, Integer>();
			for (Text val: values){
				if (!numbers.containsKey(val)){
					numbers.put(val, 0);
				}
				int count = numbers.get(val);
				numbers.put(val, count+1);
			}
			
			int maxValue = -1;
			Text finalClass = new Text("-1");
			for (Text hashkey: numbers.keySet()){
				if (maxValue<numbers.get(hashkey)){
					maxValue = numbers.get(hashkey);
					finalClass = hashkey;
				}
			}
			context.write(key,finalClass);
		}
	}
	
	public void main(String args[])throws Exception{
		Configuration conf = new Configuration();
		args = new String[2];
		args[0]=new String("hdfs://192.168.75.128:9000/user/root/in");
		args[1]=new String("hdfs://192.168.75.128:9000/user/root/out");
	    Job job = new Job(conf, "KNN Initiate");  
	    job.setJarByClass(KNN.class);  
	    job.setMapperClass(InitialTrainDataMapper.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));  
	    FileOutputFormat.setOutputPath(job,new Path(args[1]));    
	    job.waitForCompletion(true);
	    System.out.println("Initial Ok");
	    
	    Configuration newconf = new Configuration();
		String[] newargs = new String[2];
		newargs[0]=new String("hdfs://192.168.75.128:9000/user/root/in");
		newargs[1]=new String("hdfs://192.168.75.128:9000/user/root/result");
	    Job newjob = new Job(newconf, "KNN");  
	    newjob.setJarByClass(KNN.class);  
	    newjob.setMapperClass(KNNMapper.class);
	    newjob.setReducerClass(KNNReducer.class);
	    newjob.setMapOutputKeyClass(IntWritable.class);
	    newjob.setMapOutputValueClass(Text.class);
	    newjob.setOutputKeyClass(IntWritable.class);  
	    newjob.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(newjob, new Path(newargs[0]));  
	    FileOutputFormat.setOutputPath(newjob,new Path(newargs[1]));    
	    newjob.waitForCompletion(true);
	    System.out.println("Classification Ok");
	}
}
