package com.jorgefigueiredo.demos.hadoop.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNWordsMapper extends Mapper<Text, Text, Text, LongWritable> {

	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] line_words = value.toString().split(" "); // Must be replaced by a better way to split the string

		for(String word : line_words) {
			if(word != null && word != "") {
				context.write(new Text(word), new LongWritable(1));
			}
		}
		
	}
	
}
