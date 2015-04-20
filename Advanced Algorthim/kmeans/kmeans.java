package myKmeans;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Kmeans {
		static int totalClusters = 5;
		public static ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();
		
		//the InitialCenterMapper is used to initialize the center
		public static class InitialCenterMapper
		extends Mapper<Object, Text, NullWritable, Text>{
			int k = 1;
			
			@Override
			protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
				ArrayList<Double> list = new ArrayList<Double>();
				String [] line = value.toString().split(",");
				for (String item: line){
					list.add(Double.parseDouble(item));
					}
				if (k<=totalClusters){
					centers.add(list);
				}
				else{
					double replaceProb = 1.0/(1+totalClusters);
					if (replaceProb>Math.random()){
						int replaceCenterId = (int) (Math.random()*totalClusters);
						centers.remove(replaceCenterId);
						centers.add(list);
					}
				}
				if (k==1){
					context.write(NullWritable.get(),new Text("Data has been initialized"));
				}
				k++;
			}	
		}

		//the kmeansMapper is used to assign the nodes to corresponding cluster
		public static class kmeansMapper
		extends Mapper<Object, Text, IntWritable, Text>{
			
		protected static double Cosdistance(ArrayList<Double> pointA, ArrayList<Double> pointB){
			double distance;
			double powerA = 0.0;
			double powerB = 0.0;
			double powerAB = 0.0;
			for (int i=0; i<pointA.size();i++){
				powerA += pointA.get(i)*pointA.get(i);
				powerB += pointB.get(i)*pointB.get(i);
				powerAB += pointA.get(i)*pointB.get(i);
			}
			distance = (double)(1-powerAB/(Math.sqrt(powerA)*Math.sqrt(powerB)));
			return distance;
		}
	
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			double dis = 9999.0;
			ArrayList<Double> list = new ArrayList<Double>();
			String [] line = value.toString().split(",");
			for (String itme: line){
				list.add(Double.parseDouble(itme));
			}
			int centerID = -1;
			double tempDis;
			for (int i=0; i<totalClusters; i++){
				tempDis = Cosdistance(list, centers.get(i));
				if (dis>tempDis){
					dis = tempDis;
					centerID = i;
				}
			}
			context.write(new IntWritable(centerID), value);
			}	
		}

		//the kmeansReducer is used to update centers
		public static class kmeansReducer extends
		Reducer<IntWritable, Text, IntWritable, Text>{
			static int dimension = centers.get(0).size();
			@Override
			protected void setup(Context context){
				centers.clear();
			}
			@Override
			protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException{
				int numofPoints=0;
				ArrayList<Double> center = new ArrayList<Double>();
				double [] list = new double[dimension];
				for (int i=0; i<dimension; i++){
					list[i] = 0.0;
				}
				
				
				for (Text val: values){
					String [] line = val.toString().split(",");
					for (int j=0; j<dimension; j++){
						list[j]+=Double.parseDouble(line[j]);
					}
					numofPoints+=1;
				}
				
				for (int k=0; k<dimension; k++){
					center.add(list[k]*1.0/numofPoints);
				}
				centers.add(center);
				context.write(key, new Text(center.toString()));
				}
			}
	
		public static void main(String args[]) throws Exception {
			Configuration conf = new Configuration();
			args = new String[2];
			args[0]=new String("hdfs://192.168.75.128:9000/user/root/in0");
			args[1]=new String("hdfs://192.168.75.128:9000/user/root/out");
		    Job job = new Job(conf, "Kmeans Initiate");  
		    job.setJarByClass(Kmeans.class);  
		    job.setMapperClass(InitialCenterMapper.class);
		    job.setOutputKeyClass(NullWritable.class);  
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));  
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));    
		    job.waitForCompletion(true);
		    System.out.println("Ok");
		    		    
			int loop;
			int iternum = 20;
			for(loop= 0; loop<iternum; loop++){
				Configuration newconf = new Configuration();
				String[] newargs = new String[2];
				newargs[0]=new String("hdfs://192.168.75.128:9000/user/root/in0");
				newargs[1]=new String("hdfs://192.168.75.128:9000/user/root/in"+
				String.valueOf(loop+1));
			    Job newjob = new Job(newconf, "kmeans");  
			    newjob.setJarByClass(Kmeans.class);  
			    newjob.setMapperClass(kmeansMapper.class);
			    newjob.setReducerClass(kmeansReducer.class);
			    newjob.setMapOutputKeyClass(IntWritable.class);
			    newjob.setMapOutputValueClass(Text.class);
			    newjob.setOutputKeyClass(IntWritable.class);  
			    newjob.setOutputValueClass(Text.class);  
			    FileInputFormat.addInputPath(newjob, new Path(newargs[0]));  
			    FileOutputFormat.setOutputPath(newjob,new Path(newargs[1]));    
			    newjob.waitForCompletion(true);
			    System.out.println(String.valueOf(loop+1)+" Time");
			}
			
			Configuration thirdconf = new Configuration();
			String [] thirdargs = new String[2];
			thirdargs[0]=new String("hdfs://192.168.75.128:9000/user/root/in0");
			thirdargs[1]=new String("hdfs://192.168.75.128:9000/user/root/finalresult");
		    Job thirdjob = new Job(thirdconf, "partition result");  
		    thirdjob.setJarByClass(Kmeans.class);  
		    thirdjob.setMapperClass(kmeansMapper.class);
		    thirdjob.setOutputKeyClass(IntWritable.class);  
		    thirdjob.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(thirdjob, new Path(thirdargs[0]));  
		    FileOutputFormat.setOutputPath(thirdjob,new Path(thirdargs[1]));    
		    thirdjob.waitForCompletion(true);
		    System.out.println("Ok");
	    }  
}
			


