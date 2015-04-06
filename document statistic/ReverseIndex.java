package reverseIndex;
import java.io.IOException;   

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;

public class IndexReverse{
   enum Counter{
	   LINKSKIP;
   }
   //对每一行数据进行分隔，并求和  
   public static class SplitMapper extends  
           Mapper<Object, Text, Text, Text> {    
       public void map(Object key, Text value, Context context)  
               throws IOException, InterruptedException { 
    	   try{
    	   String [] line = value.toString().split(" ");
    	   String firstColumn = new String(line[0]);
    	   String secondColumn = new String(line[1]);
           context.write(new Text(firstColumn), new Text(secondColumn));
    	   }
    	   catch (java.lang.ArrayIndexOutOfBoundsException e){
    		   context.getCounter(Counter.LINKSKIP).increment(1);
    	   }
       } 
   }  
     
   // 汇总求和，输出  
   public static class SumReducer extends  
           Reducer<Text, Text, Text, Text> {  
       public void reduce(Text key, Iterable<Text> values,  
               Context context) throws IOException, InterruptedException {
    	   String valueString;
    	   String out = "";
    	   LinkedList<Integer> list = new LinkedList<Integer>(); 
    	   int intValue;
    	   for (Text val: values){
    		   valueString = val.toString();
    		   intValue = Integer.parseInt(valueString);
    		   list.add(intValue);
    	   }
    	   Collections.sort(list, null);
    	   for (int ele: list){
    		   out += Integer.toString(ele) +"|"; 
    	   }
    	   context.write(key, new Text(out));
       }
   }  
   /** 
    * @param args 
    * @throws Exception 
    */  
   public static void main(String[] args) throws Exception {  
       Configuration conf = new Configuration();  
       String[] otherArgs = new GenericOptionsParser(conf, args)  
               .getRemainingArgs();  
       if (otherArgs.length != 2) {  
           System.err.println("Usage: numbersum <in> <out>");  
           System.exit(2);  
       }  
       Job job = new Job(conf, "reverse Index");  
       job.setJarByClass(IndexReverse.class);  
       job.setMapperClass(SplitMapper.class);  
       job.setReducerClass(SumReducer.class);  
       job.setOutputKeyClass(Text.class);  
       job.setOutputValueClass(Text.class);  
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));  
       System.exit(job.waitForCompletion(true) ? 0 : 1);  
       System.out.println("ok");  
   }  

}
