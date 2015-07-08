package com.jorgefigueiredo.demos.hadoop.console;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.jorgefigueiredo.demos.hadoop.utils.ConfigUtils;

public class LocalConfigurationApp {

	public static void main(String[] args) {
		
		
		Configuration configuration = ConfigUtils.getLocalConfiguration(new Configuration());
		
		for(Entry<String, String> item : configuration) {
			
			System.out.println(String.format("%s-%s", item.getKey(), item.getValue()));
			
		}

	}

}
