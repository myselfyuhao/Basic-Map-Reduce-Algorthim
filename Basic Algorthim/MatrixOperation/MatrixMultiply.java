/*This file calulates the matrix multiply:
Where the text file first.txt denotes the first matrix
the text file second.txt denotes the second matrix
The matrix is stored as triple:

The pattern for the matrix multiply is:
(rNumber*jNumber)*(jNumber*cNumber)
*/

package matrixMlp;
import java.io.*;

import joinManipulation.JoinManipulation;
import joinManipulation.JoinManipulation.DataJoinMapper;
import joinManipulation.JoinManipulation.DataJoinReducer;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMlp {
	
	public static class MatrixMlpMapper
	extends Mapper<Object, Text, Text, Text>{
		private String file = new String();
		private Text map_key = new Text();
		private Text map_value = new Text();
	    int rNumber = 4;
	    int cNumber = 2;
	    
	    
		@Override
		protected void setup(Context context){
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			file = new String(filename);
		}
		
		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			String [] line = value.toString().split("\t");
			if (file.equals("first.txt")){
				for (int k=1; k<cNumber; k++){
					map_key.set(line[0]+"\t"+String.valueOf(k));
					map_value.set("M"+"\t"+line[1]+"\t"+line[2]);
				}
			}else if(file.equals("second.txt")){
		          for(int i = 1; i<=rNumber; i++){
		              map_key.set(String.valueOf(i) + "\t" +line[1]);
		              map_value.set("N" + "\t" + line[0] + "\t" + line[2]);
		              context.write(map_key, map_value);
		          }	
			}
		}
	}

	public static class MatrixMlpReducer 
	extends Reducer<Text,Text, Text, Text>{
		int jNumber = 3;
		int M_ij[]= new int[jNumber+1]; 
		int N_jk[]= new int[jNumber+1];
		int j, ij, jk;
		int jsum=0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException{
			for (Text val: value){
				String [] element = val.toString().split("\t");
				j = Integer.parseInt(element[1]);
				if (element[0].equals("M")){
					ij = Integer.parseInt(element[2]);
					M_ij[j] = ij;
				}else if(element[0].equals("N")){
					jk = Integer.parseInt(element[2]);
					N_jk[j] = jk;
				}
			}
			
			for (int i=0; i<jNumber; i++){
				jsum+=M_ij[i]*N_jk[i];
			}
			context.write(key, new Text(String.valueOf(jsum)));
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
	    job.setJarByClass(MatrixMlp.class);  
	    job.setMapperClass(MatrixMlpMapper.class);
	    job.setReducerClass(MatrixMlpReducer.class);
	    job.setOutputKeyClass(Text.class);  
	    job.setOutputValueClass(Text.class);  
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
	    System.out.println("ok");  
	    }  
}