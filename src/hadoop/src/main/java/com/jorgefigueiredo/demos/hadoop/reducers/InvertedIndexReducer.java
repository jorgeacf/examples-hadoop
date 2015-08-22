package com.jorgefigueiredo.demos.hadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		StringBuilder sb = new StringBuilder();
		
		for(Text line : values) {
			sb.append(line + " ");
		}
		
		context.write(key, new Text(sb.toString()));
	}
	
}
