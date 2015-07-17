package com.jorgefigueiredo.demos.hadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TestMapperTextInputFormat extends Mapper<LongWritable, Text, LongWritable, Text> {

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String[] line = value.toString().split(",");
		
		context.write(new LongWritable(Integer.parseInt(line[0])), new Text(line[1]));
	
	}
}
