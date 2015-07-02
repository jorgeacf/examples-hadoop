package com.jorgefigueiredo.demos.hdfs.files;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFileFromHDFS {

	public static void main(String[] args) throws IOException {
		

		String uri = "file1.txt";
		Path path = new Path(uri);
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		
		//conf.addResource(new Path("/opt/apache_hadoop/etc/hadoop/core-site.xml"));
		
		FileSystem fs = FileSystem.get(conf);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		String line;
		line = br.readLine();
		
		while(line != null) {
			System.out.println(line);
			
			
			String[] lineSegments = line.split("\t");
			
			if(lineSegments != null && lineSegments.length == 2) {
				
				//System.out.println("Map: " + lineSegments[0] + ", " + lineSegments[1]);
				
			}
			
			line=br.readLine();
		}
				
		
	}

}
