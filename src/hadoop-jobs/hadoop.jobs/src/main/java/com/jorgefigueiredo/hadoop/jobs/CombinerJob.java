package com.jorgefigueiredo.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class CombinerJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new CombinerJob(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception {
	
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CombinerJob.class);
		
		job.setMapperClass(MapperFunction.class);
		job.setReducerClass(ReducerFunction.class);
		job.setCombinerClass(ReducerFunction.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		FileInputFormat.setMaxInputSplitSize(job, 142 / 2); // To get two mapper instances
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class MapperFunction extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split(",");
			
			context.write(new Text(tokens[1]), new LongWritable(1));
		}
	}
	
	public static class ReducerFunction extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			long sum = 0;
			for(LongWritable value : values) {
				sum += value.get();
			}
			
			context.write(key, new LongWritable(sum));
		}
		
	}
	
}
