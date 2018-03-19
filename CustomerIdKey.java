import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class CustomerIdKey implements WritableComparable<CustomerIdKey>{
	
	public LongWritable customerId=new LongWritable();
	public IntWritable recordType=new IntWritable();
	
	public static final IntWritable sep2014 = new IntWritable(0);
	public static final IntWritable oct2014 = new IntWritable(1);

	public CustomerIdKey() {}
	public CustomerIdKey(long customerId, IntWritable recordType){
		this.customerId.set(customerId);
		this.recordType = recordType;
		
	}
	
	
	public void write(DataOutput out) throws IOException {
	    this.customerId.write(out);
	    this.recordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    this.customerId.readFields(in);
	    this.recordType.readFields(in); 
	}
	
	public int compareTo(CustomerIdKey other) {	
		if (this.customerId.equals(other.customerId )) {
	        return this.recordType.compareTo(other.recordType);
	    } else {
	        return this.customerId.compareTo(other.customerId);
	    }
		
		
	}
	
	public boolean equals (CustomerIdKey other) {
	    return this.customerId.equals(other.customerId) && this.recordType.equals(other.recordType );
	}

	public int hashCode() {
	    return this.customerId.hashCode();
	}
	
	


	

}
