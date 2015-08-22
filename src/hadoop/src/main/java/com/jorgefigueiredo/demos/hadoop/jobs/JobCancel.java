package com.jorgefigueiredo.demos.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JobCancel extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new JobCancel(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		final Job job = Job.getInstance(conf);
		
		job.setJobName("JobCancel");
		job.setJarByClass(JobCancel.class);
		
		
		job.setMapperClass(JobCancelMapper.class);
		job.setReducerClass(JobCancelReducer.class);
		
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input/simple"));
		job.setInputFormatClass(TextInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output/simple"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		new Thread(new Runnable() {

			public void run() {
				
				
				State state = null;
				
				do {
					try {
						
						state = job.getJobState();
						
						
					} catch (Exception e) {
						//e.printStackTrace();
						
						// Just ignore in this example. If the method getJobState is called 
						// before the job is in a valid state an exception is thrown.
						
					} 
					
					
					if(state == State.RUNNING) {
						break;
					}
					
					System.out.println("[CANCEL] Waiting for job to start... JobStatus=[" + state + "]");
					
					try {
						
						Thread.sleep(3 * 1000);
						
					} catch (InterruptedException e) {
						
						e.printStackTrace();
					}
					
				}
				while(true);
				
				try {
					System.out.println("[CANCEL] About to kill the job...");
					job.killJob();
					System.out.println("[CANCEL] Job killed...");
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			
		}).start();
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}



class JobCancelMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		String[] line = value.toString().split(",");
		
		context.write(new LongWritable(Integer.parseInt(line[0])), new Text(line[1]));
	
	}

}


class JobCancelReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		

		List<String> valuesList = new ArrayList<String>();
		
		for(Text text : values) {
			valuesList.add(text.toString());
		}
		
		context.write(key, new Text(String.join("-", valuesList)));
		
	}
}
