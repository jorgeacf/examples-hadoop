package com.jorgefigueiredo.hadoop.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.jorgefigueiredo.hadoop.joins.MapperJoinDistributedCacheJob;

public class MapperJoinDistributedCacheJobTests {

	MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
	
	
	@Before
	public void setUp() {
		
		MapperJoinDistributedCacheJob.MapperFunction mapper = new MapperJoinDistributedCacheJob.MapperFunction();
		
		mapDriver = MapDriver.newMapDriver(mapper);
	}
	
	
	@Test
	public void testMapper() throws IOException {
		
		mapDriver.withInput(new LongWritable(), new Text("1,1,value1"));
		mapDriver.withInput(new LongWritable(), new Text("2,2,value2"));
		mapDriver.withInput(new LongWritable(), new Text("3,3,value3"));
	    
		mapDriver.withOutput(new LongWritable(1), new Text("Jorge,value1"));
		mapDriver.withOutput(new LongWritable(2), new Text("Adam,value2"));
		mapDriver.withOutput(new LongWritable(3), new Text("Chris,value3"));
		
	    mapDriver.runTest();
	}
	

}
