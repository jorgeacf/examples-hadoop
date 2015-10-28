package com.jorgefigueiredo.hadoop.jobs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.jorgefigueiredo.hadoop.joins.ReduceJoinOneToOneJob;
import com.jorgefigueiredo.hadoop.joins.ReduceJoinOneToOneJob.CustomKey;
import com.jorgefigueiredo.hadoop.joins.ReduceJoinOneToOneJob.CustomValue;

public class ReduceJoinOneToOneJobTests {

	MapDriver<LongWritable, Text, CustomKey, CustomValue> mapDriver;
	ReduceDriver<CustomKey, CustomValue, NullWritable, Text> reduceDriver;
	
	MapReduceDriver<LongWritable, Text, CustomKey, CustomValue, NullWritable, Text> mapReduceDriver;
	
	
	@Before
	public void setUp() {
		
		ReduceJoinOneToOneJob.UserMapperFunction userMapper = new ReduceJoinOneToOneJob.UserMapperFunction();
		ReduceJoinOneToOneJob.CommentMapperFunction commentMapper = new ReduceJoinOneToOneJob.CommentMapperFunction();
		
		ReduceJoinOneToOneJob.ReducerFunction reducer = new ReduceJoinOneToOneJob.ReducerFunction();
		
		mapDriver = MapDriver.newMapDriver(userMapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(userMapper, reducer);
		
		mapReduceDriver = MapReduceDriver.newMapReduceDriver();
		
	}
	
	
	@Test
	public void testMapper() throws IOException {
	
		
		mapDriver.withInput(new LongWritable(), new Text("1,name1,value1"));
		mapDriver.withInput(new LongWritable(), new Text("1,name1,value2"));
		mapDriver.withInput(new LongWritable(), new Text("1,name1,value3"));
		/*
	    mapDriver.withOutput(new CustomKey(1, "name1"), new Text("value1"));
	    mapDriver.withOutput(new CustomKey(1, "name1"), new Text("value2"));
	    mapDriver.withOutput(new CustomKey(1, "name1"), new Text("value3"));
	    */
	    mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		
		List<Text> values = new LinkedList<Text>();
		values.add(new Text("value1"));
		values.add(new Text("value2"));
		values.add(new Text("value3"));
		
		//reduceDriver.withInput(new CustomKey(1, "name1"), values);
		
	    reduceDriver.withOutput(NullWritable.get(), new Text("value1value2value3"));
		
	    reduceDriver.runTest();
	}

}
