package com.jorgefigueiredo.demos.hadoop.utils;

import java.io.File;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;

public class ConfigUtils {

	
	public static Configuration getLocalConfiguration(Configuration configuration) {
		
		
		FileSystem fs = FileSystems.getDefault();
		
		Path pathCoreSite = fs.getPath("config", "core-site.xml");
		Path pathHdfsSite = fs.getPath("config", "hdfs-site.xml");
		
		File coreSite = pathCoreSite.toFile();
		File hdfsSite = pathHdfsSite.toFile();
		
		if(coreSite.exists()) {
			configuration.addResource(new org.apache.hadoop.fs.Path(coreSite.toURI()));	
		}
		
		if(hdfsSite.exists()) {
			configuration.addResource(new org.apache.hadoop.fs.Path(hdfsSite.toURI()));
		}
		
		return configuration;
	}
	
	
}
