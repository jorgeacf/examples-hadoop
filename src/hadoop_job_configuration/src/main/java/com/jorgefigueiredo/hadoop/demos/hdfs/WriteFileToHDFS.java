package com.jorgefigueiredo.hadoop.demos.hdfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class WriteFileToHDFS {

	public static void main(String[] args) throws IOException {

		String uri = "file1.txt";
		Path path = new Path(uri);
		
		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		
		//conf.addResource(new Path("/opt/apache_hadoop/etc/hadoop/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		if(hdfs.exists(path)) {
			
			System.out.println("File already exists...");
			hdfs.delete(path, true);
			System.out.println("File deleted...");
			
		}
		
			
		OutputStream os = hdfs.create(path, new Progressable() {

			public void progress() {
				
				System.out.print(".");
				
			} });
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
		
		
		int i = 0;
		while(true) {
			
			if(i + 'a' > 'z') {
				break;
			}
			
			bw.write(String.format("%s\t%s\n", i+1, (char)(i+'a') ));
			
			i++;
		}
		
		bw.close();
		
		hdfs.close();
		
		System.out.println("File writen...");
	
		
	}

}
