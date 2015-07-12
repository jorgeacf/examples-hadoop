package com.jorgefigueiredo.demos.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JobCounters extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new JobCounters(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();

		final Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobCounters.class);
		
		job.setMapperClass(JobCountersMapper.class);
		job.setReducerClass(JobCountersReducer.class);
		
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		job.setInputFormatClass(TextInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		int result = job.waitForCompletion(true) ? 0 : 1;
		
		Counters counters = job.getCounters();
		
		Counter counter = counters.findCounter(CUSTOM_COUNTERS.NUMBER_OF_RECORDS);
		
		System.out.println("Number of records: " + counter.getValue());
		
		return result;
	}
}

enum CUSTOM_COUNTERS {
	
	NUMBER_OF_RECORDS
	
}


class JobCountersMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String[] line = value.toString().split(",");
		
		context.getCounter(CUSTOM_COUNTERS.NUMBER_OF_RECORDS).increment(1);
		
		context.write(new LongWritable(Integer.parseInt(line[0])), new Text(line[1]));
	
	}

}


class JobCountersReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		List<String> valuesList = new ArrayList<String>();
		
		for(Text text : values) {
			valuesList.add(text.toString());
		}
		
		context.write(key, new Text(String.join("-", valuesList)));
		
	}
}
