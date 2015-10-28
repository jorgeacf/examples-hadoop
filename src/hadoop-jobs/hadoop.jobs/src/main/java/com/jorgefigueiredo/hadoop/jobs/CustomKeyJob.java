package com.jorgefigueiredo.hadoop.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CustomKeyJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new CustomKeyJob(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CustomKeyJob.class);
		
		//job.setSortComparatorClass(CustomKeyComparator.class);
		
		job.setMapperClass(MapperFunction.class);
		job.setReducerClass(ReducerFunction.class);
		
		
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input
		FileInputFormat.addInputPath(job, new Path("input"));
		FileInputFormat.setMaxInputSplitSize(job, 142 / 2);
		
		job.setInputFormatClass(CustomFileInputFormat.class);
		
		// Output
		FileOutputFormat.setOutputPath(job, new Path("output"));
		//job.setOutputFormatClass(KeyValueTextOutputFormat.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	public static class CustomLineRecordReader extends RecordReader<LongWritable, Text> {

		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) 
				throws IOException, InterruptedException {
			
		       // This InputSplit is a FileInputSplit
	        FileSplit split = (FileSplit) inputSplit;
	 
	        // Retrieve configuration, and Max allowed
	        // bytes for a single record
	        Configuration job = context.getConfiguration();
	        this.maxLineLength = job.getInt(
	                "mapred.linerecordreader.maxlength",
	                Integer.MAX_VALUE);
	 
	        // Split "S" is responsible for all records
	        // starting from "start" and "end" positions
	        start = split.getStart();
	        end = start + split.getLength();
	 
	        // Retrieve file containing Split "S"
	        final Path file = split.getPath();
	        FileSystem fs = file.getFileSystem(job);
	        FSDataInputStream fileIn = fs.open(split.getPath());
	 
	        // If Split "S" starts at byte 0, first line will be processed
	        // If Split "S" does not start at byte 0, first line has been already
	        // processed by "S-1" and therefore needs to be silently ignored
	        boolean skipFirstLine = false;
	        if (start != 0) {
	            skipFirstLine = true;
	            // Set the file pointer at "start - 1" position.
	            // This is to make sure we won't miss any line
	            // It could happen if "start" is located on a EOL
	            --start;
	            fileIn.seek(start);
	        }
	 
	        in = new LineReader(fileIn, job);
	 
	        // If first line needs to be skipped, read first line
	        // and stores its content to a dummy Text
	        if (skipFirstLine) {
	            Text dummy = new Text();
	            // Reset "start" to "start + line offset"
	            start += in.readLine(dummy, 0,
	                    (int) Math.min(
	                            (long) Integer.MAX_VALUE, 
	                            end - start));
	        }
	 
	        // Position is the actual start
	        this.pos = start;
	 
			
		}
		
		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			
		       if (start == end) {
		            return 0.0f;
		        } else {
		            return Math.min(1.0f, (pos - start) / (float) (end - start));
		        }
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			
	        // Current offset is the key
	        key.set(pos);
	 
	        int newSize = 0;
	 
	        // Make sure we get at least one record that starts in this Split
	        while (pos < end) {
	 
	            // Read first line and store its content to "value"
	            newSize = in.readLine(value, maxLineLength,
	                    Math.max((int) Math.min(
	                            Integer.MAX_VALUE, end - pos),
	                            maxLineLength));
	 
	            // No byte read, seems that we reached end of Split
	            // Break and return false (no key / value)
	            if (newSize == 0) {
	                break;
	            }
	 
	            // Line is read, new position is set
	            pos += newSize;
	 
	            // Line is lower than Maximum record line size
	            // break and return true (found key / value)
	            if (newSize < maxLineLength) {
	                break;
	            }
	 
	            // Line is too long
	            // Try again with position = position + line offset,
	            // i.e. ignore line and go to next one
	            // TODO: Shouldn't it be LOG.error instead ??
	            /*
	            LOG.info("Skipped line of size " + 
	                    newSize + " at pos "
	                    + (pos - newSize));
	            */
	        }
	 
	         
	        if (newSize == 0) {
	            // We've reached end of Split
	            key = null;
	            value = null;
	            return false;
	        } else {
	            // Tell Hadoop a new line has been found
	            // key / value will be retrieved by
	            // getCurrentKey getCurrentValue methods
	            return true;
	        }
		}
		
		@Override
		public void close() throws IOException {
			
			if(in != null) {
				in.close();
			}
		}
		
	}
	
	public static class CustomFileInputFormat extends FileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			
			return new CustomLineRecordReader();
		}
		
	}

	public static class CustomKeyComparator extends WritableComparator {
		
		protected CustomKeyComparator() {
			super(CustomKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			final int BEFORE = -1;
			final int EQUAL = 0;
			final int AFTER = 1;
			
			if(a instanceof CustomKey && b instanceof CustomKey) {
				
				CustomKey keyA = (CustomKey)a;
				CustomKey keyB = (CustomKey)b;
				
				if(keyA.id < keyB.id) return AFTER;
				if(keyA.id > keyB.id) return BEFORE;
			}
			
			return EQUAL;
		}
	}
	
	public static class CustomKey implements WritableComparable<CustomKey> {

		private Integer id;
		private String name;
		
		public CustomKey() {
			
		}
		
		public CustomKey(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public void readFields(DataInput input) throws IOException {
			this.id = input.readInt();
			this.name = input.readLine();
		}

		public void write(DataOutput output) throws IOException {
			output.writeInt(this.id);
			output.writeUTF(this.name);
		}

		public int compareTo(CustomKey that) {
			
			final int BEFORE = -1;
			final int EQUAL = 0;
			final int AFTER = 1;
			
			if(this == that) return EQUAL;
			
			if(this.id < that.id) return BEFORE;
			if(this.id > that.id) return AFTER;
			
			return this.name.compareTo(that.name);
		}
		
		@Override
		public int hashCode() {
			return this.id.hashCode() * this.name.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			
			if(obj instanceof CustomKey) {
				CustomKey key = (CustomKey)obj;
				
				return this.id.equals(key.id) && this.name.equals(key.name);
				
			}
			
			return false;
		}
		
	}
	
	
	public static class MapperFunction extends Mapper<LongWritable, Text, CustomKey, Text> {
		
		private long id = 0;
		
		@Override
		protected void setup(Mapper<LongWritable, Text, CustomKey, Text>.Context context)
				throws IOException, InterruptedException {
			
			Date dt = new Date();
			id = dt.getTime();
			
			System.out.println(String.format("#################### MAPPER %d ####################", id));
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] tokens = value.toString().split(",");
			
			int id = Integer.parseInt(tokens[0]);
			String name = tokens[1];
			
			System.out.println(String.format("[----] %d - %d-%s", this.id, id, name));
			
			context.write(new CustomKey(id, name), new Text(tokens[2]));
		}
	}
	
	public static class ReducerFunction extends Reducer<CustomKey, Text, NullWritable, Text> {
		
		@Override
		protected void reduce(CustomKey key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			
			sb.append(key.id + "-" + key.name + "->");
			
			for(Text value : values) {
				sb.append(value + " ");
			}
			
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
	}
}



