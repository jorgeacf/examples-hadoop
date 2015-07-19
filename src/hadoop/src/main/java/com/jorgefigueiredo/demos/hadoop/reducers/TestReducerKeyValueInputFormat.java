package com.jorgefigueiredo.demos.hadoop.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducerKeyValueInputFormat extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		List<String> valuesList = new ArrayList<String>();
		
		for(Text text : values) {
			valuesList.add(text.toString());
		}
		
		context.write(key, new Text(String.join("-", valuesList)));
		
	}
	
}
