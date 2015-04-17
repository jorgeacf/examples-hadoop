package com.jf.hadoop_examples.main;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String line = value.toString();
		
		String[] line_segments = line.split(",");
		
		context.write(new Text(line_segments[0]), new IntWritable(Integer.parseInt(line_segments[1])));
		
	}
	
}
