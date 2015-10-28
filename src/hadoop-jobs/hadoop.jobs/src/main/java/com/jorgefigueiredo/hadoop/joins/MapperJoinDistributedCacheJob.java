package com.jorgefigueiredo.hadoop.joins;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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


public class MapperJoinDistributedCacheJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new MapperJoinDistributedCacheJob(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception {
	
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapperJoinDistributedCacheJob.class);
		
		job.setMapperClass(MapperFunction.class);
		job.setReducerClass(ReducerFunction.class);
		//job.setCombinerClass(ReducerFunction.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		//FileInputFormat.setMaxInputSplitSize(job, 142 / 2); // To get two mapper instances
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class DistributedCacheManager {
		
		public Map<Integer, String> getUsers() {
			
			Map<Integer, String> map = new HashMap<Integer, String>();
			
			// Just for tests
			map.put(1, "Jorge");
			map.put(2, "Adam");
			map.put(3, "Chris");
			map.put(4, "Lucy");
			map.put(5, "Thomas");
			
			return map;
		}
		
	}
	
	public static class MapperFunction extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private final DistributedCacheManager distributedCacheManager = new DistributedCacheManager();
		private Map<Integer, String> users;
		
		private final LongWritable outputKey = new LongWritable();
		private final Text outputValue = new Text();
		
		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			users = distributedCacheManager.getUsers();
			
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// id, userid, data
			
			String[] tokens = value.toString().split(",");
			
			outputKey.set(Long.parseLong(tokens[0]));
			
			outputValue.set(getUsernameFromCache(Integer.parseInt(tokens[1])) + "," + tokens[2]);
			
			context.write(outputKey, outputValue);
		}
		
		private String getUsernameFromCache(int userId) {
			
			if(users.containsKey(userId)) {
				return users.get(userId);
			}
			
			return null;
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
