package com.jorgefigueiredo.hbase.repository;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public abstract class AbstractHBaseRepository {

	protected final String tableName;
	protected final Configuration configuration;
	protected final HTable htable;
	
	public AbstractHBaseRepository(String tableName) throws IOException {
		this.tableName = tableName;
		this.configuration = HBaseConfiguration.create();
		
		if(tableName != null && tableName != "") {
			this.htable = new HTable(configuration, tableName);
		}
		else {
			this.htable = null;
		}
	}
	
}
