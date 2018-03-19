import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.jasper.tagplugins.jstl.core.Out;



public class Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool{

	public Driver() {
		
	}
	

public static class JoinGroupingComparator extends WritableComparator {
    public JoinGroupingComparator() {
        super (CustomerIdKey.class, true);
    }                             

    @Override
    public int compare (WritableComparable a, WritableComparable b){
    	CustomerIdKey first = (CustomerIdKey) a;
    	CustomerIdKey second = (CustomerIdKey) b;
                      
        return first.customerId.compareTo(second.customerId);
    }

}

public static class JoinSortingComparator extends WritableComparator {
    public JoinSortingComparator()
    {
        super (CustomerIdKey.class, true);
    }
                               
    @Override
    public int compare (WritableComparable a, WritableComparable b){
    	CustomerIdKey first = (CustomerIdKey) a;
    	CustomerIdKey second = (CustomerIdKey) b;
                                 
        return first.compareTo(second);
    }
}


public static class SepMapper extends Mapper<LongWritable, Text, CustomerIdKey, AtmRecord>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
        String[] recordFields = value.toString().split(",");
        long customerId = Long.parseLong(recordFields[0]);
        String atmId = recordFields[1];
        int date = Integer.parseInt(recordFields[2]);
        long time = Long.parseLong(recordFields[3]);
        double amount = Double.parseDouble(recordFields[4]);
        
                                               
        CustomerIdKey recordKey = new CustomerIdKey(customerId, CustomerIdKey.sep2014);
        AtmRecord record = new AtmRecord(customerId, atmId, date, time, amount);
           
        context.write(recordKey, record);
    }
}
               
public static class OctMapper extends Mapper<LongWritable, Text, CustomerIdKey, AtmRecord>{
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split(",");
	        long customerId = Long.parseLong(recordFields[0]);
	        String atmId = recordFields[1];
	        int date = Integer.parseInt(recordFields[2]);
	        long time = Long.parseLong(recordFields[3]);
	        double amount = Double.parseDouble(recordFields[4]);
	        
	                                               
	        CustomerIdKey recordKey = new CustomerIdKey(customerId, CustomerIdKey.oct2014);
	        AtmRecord record = new AtmRecord(customerId, atmId, date, time, amount);
	           
	        context.write(recordKey, record);
    }
}

public static class JoinReducer extends Reducer<CustomerIdKey, AtmRecord, NullWritable, Text>{
	
    public void reduce(CustomerIdKey key, Iterable<AtmRecord> values, Context context) throws IOException, InterruptedException{
        StringBuilder output = new StringBuilder();
        double sepSum = 0;
        double octSum = 0;
                                               
        for (AtmRecord v : values) {
            
            if (key.recordType.equals(CustomerIdKey.sep2014)){
                
            	sepSum += v.amount.get();
               // output.append(Integer.parseInt(key.customerId.toString())).append(", ");
                
            } else {
                octSum += v.amount.get();
                     
            }
            
        }
        
        output.append(key.customerId.toString() + ", " + sepSum + ", " + octSum);
        if(sepSum > octSum)
        {
        	 context.write(NullWritable.get(), new Text(output.toString()));
        }
        
         
      
        
    }
}

@Override
public int run(String[] allArgs) throws Exception {
	String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
    
    Job job = Job.getInstance(getConf());
    job.setJarByClass(Driver.class);
                               
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
                               
    job.setMapOutputKeyClass(CustomerIdKey.class);
    job.setMapOutputValueClass(AtmRecord.class);
                               
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SepMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OctMapper.class);
                              
    job.setReducerClass(JoinReducer.class);
                         
    job.setSortComparatorClass(JoinSortingComparator.class);
    job.setGroupingComparatorClass(JoinGroupingComparator.class);
                               
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
                               
    FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
    boolean status = job.waitForCompletion(true);
    if (status) {
        return 0;
    } else {
        return 1;
    }             
	
}

public static void main(String[] args) throws Exception{                               
    Configuration conf = new Configuration();
    int res = ToolRunner.run(new Driver(), args);
}



}
