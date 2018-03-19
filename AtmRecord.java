import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class AtmRecord implements Writable{

	public LongWritable customerId = new LongWritable();
	public Text atmId = new Text();
	public IntWritable date = new IntWritable();
	public LongWritable time = new LongWritable();
	public DoubleWritable amount = new DoubleWritable();
	

	public AtmRecord(){	}
	
	public AtmRecord(long customerId,String atmId,int date,long time,double amount){
		this.customerId.set(customerId);
		this.atmId.set(atmId);
		this.date.set(date);
		this.time.set(time);
		this.amount.set(amount);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.customerId.readFields(in);
		this.atmId.readFields(in);
		this.date.readFields(in);
		this.time.readFields(in);
		this.amount.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.customerId.write(out);
		this.atmId.write(out);
		this.date.write(out);
		this.time.write(out);
		this.amount.write(out);
		
	}

}
