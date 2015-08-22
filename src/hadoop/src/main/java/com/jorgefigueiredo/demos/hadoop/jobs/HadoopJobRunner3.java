package com.jorgefigueiredo.demos.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HadoopJobRunner3 extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new HadoopJobRunner3(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(HadoopJobRunner3.class);
		
		job.setMapperClass(TestMapper.class);
		job.setReducerClass(TestReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input/simple"));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output/simple"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}


class TestMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void cleanup(Mapper<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		System.err.println("##HadoopJobRunner3###### Mapper-Cleanup  #########\n\n\n");
	};
	
	@Override
	protected void setup(Mapper<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		System.err.println("######## Mapper-Setup #########\n\n\n");
	};
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		System.err.println("######## Mapper - Map  #########\n\n\n");
		context.write(key, value);
	
	}
}


class TestReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Reducer<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		System.err.println("######## Reducer - Setup  #########\n\n\n");
	};
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		System.err.println("######## Reducer - Reduce  #########\n\n\n");
		
		List<String> valuesList = new ArrayList<String>();
		
		for(Text text : values) {
			valuesList.add(text.toString());
		}
		
		context.write(key, new Text(String.join("-", valuesList)));
		
	}
}
