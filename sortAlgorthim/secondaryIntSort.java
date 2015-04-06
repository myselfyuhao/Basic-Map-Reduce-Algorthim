package secondSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SecondaryIntSort {
    public static class IntPair implements WritableComparable<IntPair>
    {
        private int first;
        private int second;
        
        public void set(int firstInt, int secondInt)
        {
            first = firstInt;
            second = secondInt;
        }
        
        public int getFirst()
        {
            return first;
        }
        
        public int getSecond()
        {
            return second;
        }
        
        @Override
        public void readFields(DataInput in) throws IOException
        {
            first = in.readInt();
            second = in.readInt();
        }
        
        @Override
        public void write(DataOutput out) throws IOException
        {
            out.writeInt(first);
            out.writeInt(second);
        }
        
        @Override
        public int compareTo(IntPair o)
        {
            //Ascending sort
            if (first != o.first)
            {
                return first < o.first ? -1 : 1;
            }
            else if (second != o.second)
            {
                return second < o.second ? -1 : 1;
            }
            else
            {
                return 0;
            }
        }

        @Override
        //ReWrite the Partition Method
        public int hashCode()
        {
            return first * 157 + second;
        }
        
        @Override
        public boolean equals(Object right)
        {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair)
            {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            }
            else
            {
                return false;
            }
        }
    }
    
    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable>
    {
        @Override
        public int getPartition(IntPair key, IntWritable value,int numPartitions)
        {
            return Math.abs(key.getFirst() * 127) % numPartitions;
        }
    }

    
    public static class GroupingComparator extends WritableComparator
    {
        protected GroupingComparator()
        {
            super(IntPair.class, true);
        }
        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2)
        {
        	//Ascending sort
            IntPair p1 = (IntPair) w1;
            IntPair p2 = (IntPair) w2;
            int Fst = p1.getFirst();
            int Sec = p2.getFirst();
            return Fst == Sec ? 0 : (Fst < Sec ? -1 : 1);
        }
    }

    public static class IntSortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>
    {
        private final IntPair intPairkey = new IntPair();
        private final IntWritable SecondColumn = new IntWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] line = value.toString().split(",");            
            intPairkey.set(Integer.parseInt(line[0]), Integer.parseInt(line[1]));
            SecondColumn.set(Integer.parseInt(line[1]));
            context.write(intPairkey, SecondColumn);
        }
    }

    public static class IntSortReducer extends Reducer<IntPair, IntWritable, Text, IntWritable>
    {
        private final Text left = new Text();
     
        public void reduce(IntPair key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
        {
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values)
            {
                context.write(left, val);
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
       Configuration conf = new Configuration();  
	   String[] otherArgs = new GenericOptionsParser(conf, args)  
	            .getRemainingArgs();  
	   if (otherArgs.length != 2) {  
	       System.err.println("Usage: numbersum <in> <out>");  
	       System.exit(2);  
	   }  
	   Job job = new Job(conf, "secondarysort");
	   job.setJarByClass(SecondaryIntSort.class);     
	   job.setMapperClass(IntSortMapper.class);
	   job.setReducerClass(IntSortReducer.class);
	   job.setPartitionerClass(FirstPartitioner.class);    
	   job.setGroupingComparatorClass(GroupingComparator.class);
	   job.setMapOutputKeyClass(IntPair.class);
	   job.setMapOutputValueClass(IntWritable.class);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
	   job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
     FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
     FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
     System.out.println("OK");
    }
}
