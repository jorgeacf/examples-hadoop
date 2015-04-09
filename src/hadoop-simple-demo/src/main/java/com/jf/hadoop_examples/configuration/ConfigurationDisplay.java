package com.jf.hadoop_examples.configuration;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationDisplay extends Configured implements Tool {

	static {
		
		Configuration.addDefaultResource("hadoop-local.xml");
		
	}
	
	public int run(String[] args) throws Exception {
		
		Configuration configuration = getConf();
		
		for(Entry<String, String> entry: configuration) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new ConfigurationDisplay(), args);
		System.exit(exitCode);
		
	}

}
