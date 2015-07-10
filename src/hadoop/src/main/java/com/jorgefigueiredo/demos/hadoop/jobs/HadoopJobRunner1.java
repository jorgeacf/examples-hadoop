package com.jorgefigueiredo.demos.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jorgefigueiredo.demos.hadoop.mappers.TestMapperTextInputFormat;
import com.jorgefigueiredo.demos.hadoop.reducers.TestReducerTextInputFormat;


public class HadoopJobRunner1 extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new HadoopJobRunner1(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(HadoopJobRunner1.class);
		
		
		job.setMapperClass(TestMapperTextInputFormat.class);
		job.setReducerClass(TestReducerTextInputFormat.class);
		
		
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

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
