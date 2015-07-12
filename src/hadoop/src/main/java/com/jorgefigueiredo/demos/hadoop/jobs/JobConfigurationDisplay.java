package com.jorgefigueiredo.demos.hadoop.jobs;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobConfigurationDisplay extends Configured implements Tool {

	
    public static void main( String[] args ) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new JobConfigurationDisplay(), args);
        System.exit(res);
    }
	
	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();
		
		Iterator<Entry<String, String>> iterator = configuration.iterator();
		while(iterator.hasNext()) {
			
			Entry<String, String> item = iterator.next();
			
			System.out.println(item.getKey() + "=" + item.getValue());
			
		}
		
		return 0;
	}

}
