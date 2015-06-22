package com.jorgefigueiredo.hadoop.demos.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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


public class JobWithDistributedCache extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new JobWithDistributedCache(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance();
		job.addCacheArchive(new URI("cache/file1.txt"));
		
		
		job.setJarByClass(JobWithDistributedCache.class);
		
		
		job.setMapperClass(MapperWithDistributedCache.class);
		job.setReducerClass(ReducerWithDistributedCache.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}


class MapperWithDistributedCache extends Mapper<Text, Text, Text, Text> {
	
	
	private HashMap<Integer, String> map = new HashMap<Integer, String>();
	
	private void LoadMapValues(Context context) throws IOException {
		
		URI[] cacheArchives = context.getCacheArchives();
		
		for(URI uri : cacheArchives) {
			
			if(uri.getRawPath().contains("file1.txt")) {
				
				
				Path path = new Path(uri);
				
				FileSystem fs = FileSystem.get(context.getConfiguration());
				
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				
				String line = br.readLine();
				
				while(line != null) {
					
					String[] lineSegments = line.split("\t");
					
					
					if(lineSegments != null && lineSegments.length == 2) {
						
						map.put(Integer.parseInt(lineSegments[0]), lineSegments[1]);
						
					}
					
					line = br.readLine();
					
				}
				break;
			}
			
		}
		
	}
	
	
	@Override
	protected void setup(Mapper<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		
		LoadMapValues(context);
		
	};

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		
		context.write(key, value);
	
	}
	
	@Override
	protected void cleanup(Mapper<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		
	};
}


class ReducerWithDistributedCache extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void setup(Reducer<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException 
	{
		
	};
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		List<String> valuesList = new ArrayList<String>();
		
		for(Text text : values) {
			valuesList.add(text.toString());
		}
		
		context.write(key, new Text(String.join("-", valuesList)));
		
	}
}
