package com.jorgefigueiredo.demos.hadoop.reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.jorgefigueiredo.demos.hadoop.utils.ConvertUtils;

public class TopNWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private Map<String, Integer> countMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
	
		int count = 0;
		
		for(LongWritable c : values) {
			count += c.get();
		}
	
		countMap.put(key.toString(), count);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	
	
		Map<String, Integer> sortedMap = ConvertUtils.convert(countMap);
		
		
		int top = 20;
		
		for(String key : sortedMap.keySet()) {
			
			if(top <= 0) { break; }
			
			context.write(new Text(key), new  LongWritable(sortedMap.get(key)));
			
			top--;
		}
	
	}
	
}
