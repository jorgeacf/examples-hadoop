package com.jorgefigueiredo.hadoop.joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReduceJoinOneToOneJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new ReduceJoinOneToOneJob(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception {
	
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(UserMapperFunction.class);
		job.setReducerClass(ReducerFunction.class);
		//job.setCombinerClass(ReducerFunction.class);
		
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(CustomValue.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		//FileInputFormat.setMaxInputSplitSize(job, 142 / 2); // To get two mapper instances
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class CustomKey implements Writable, WritableComparable<CustomKey> {

		private Text key = new Text();
		private IntWritable tag = new IntWritable();
		
		public CustomKey() { }
		
		public void set(String key, int tag) {
			this.key.set(key);
			this.tag.set(tag);
		}
		
		public Text getKey() {
			return key;
		}
		
		public IntWritable getTag() {
			return tag;
		}
		
		public int compareTo(CustomKey key) {
			
			int compareValue = this.key.compareTo(key.getKey());
			
			if(compareValue == 0) {
				compareValue = this.tag.compareTo(key.getTag());
			}
			
			return compareValue;
		}

		public void readFields(DataInput in) throws IOException {
			key.readFields(in);
			tag.readFields(in);
		}

		public void write(DataOutput out) throws IOException {
			key.write(out);
			tag.write(out);
		}
		
	}
	
	public static class CustomValue implements Writable {

		private int tag;
		private String value;
		
		public CustomValue() { }
		
		public void set(int tag, String value) {
			this.tag = tag;
			this.value = value;
		}
		
		public void readFields(DataInput in) throws IOException {
			tag = in.readInt();
			value = in.readUTF();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(tag);
			out.writeUTF(value);
		}
		
	}
	
	
	public static class CustomJoiningPartitioner extends Partitioner<CustomKey, Text> {

		@Override
		public int getPartition(CustomKey key, Text value, int numPartitions) {
			
			return key.getKey().hashCode() % numPartitions;
		}
		
	}
	
	public static class UserMapperFunction extends Mapper<LongWritable, Text, CustomKey, CustomValue> {
		
		private CustomKey customKey = new CustomKey();
		private CustomValue customValue = new CustomValue();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\t");
			
			customKey.set(tokens[0], 0);
			customValue.set(1, tokens[1]);
			
			context.write(customKey, customValue);
		}
	}
	
	public static class CommentMapperFunction extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\t");
			
			context.write(new Text(tokens[1]), new LongWritable(1));
		}
	}
	
	public static class ReducerFunction extends Reducer<CustomKey, CustomValue, NullWritable, Text> {
		
		private Text joinedText = new Text();
		private StringBuilder sb = new StringBuilder();
		private NullWritable nullKey = NullWritable.get();
		
		@Override
		protected void reduce(CustomKey key, Iterable<CustomValue> values, Context context)
				throws IOException, InterruptedException {
			/*
			long sum = 0;
			for(LongWritable value : values) {
				sum += value.get();
			}
			*/
			//context.write(key, new LongWritable(sum));
		}
		
	}
	
}
