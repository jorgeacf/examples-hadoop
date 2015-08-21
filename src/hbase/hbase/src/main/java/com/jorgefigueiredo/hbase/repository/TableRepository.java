package com.jorgefigueiredo.hbase.repository;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TableRepository extends AbstractHBaseRepository implements ITableRepository {

	private static final Logger logger = LogManager.getLogger(TableRepository.class);
	
	public TableRepository() throws IOException {
		super("");
	}
	
	
	public boolean containsTable(String tableName) {
		
		logger.trace("containsTable()");
		
		HBaseAdmin hbaseAdmin =  null;
		boolean exists = false;
		try {
			
			hbaseAdmin = new HBaseAdmin(configuration);
			
			exists = hbaseAdmin.tableExists(tableName);
			
			
		} catch (IOException e) {
			logger.error("IOException", e);
		}
		
		return exists;
	}

	public void createTable(String tableName, String[] columnFamilies) {
		
		logger.trace("createTable()");
		
		HBaseAdmin hbaseAdmin =  null;
		
		try {
			
			hbaseAdmin = new HBaseAdmin(configuration);
			
			if(hbaseAdmin.tableExists(tableName)) { return; };
			
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			
			for(String columnFamily : columnFamilies) {
				desc.addFamily(new HColumnDescriptor(columnFamily));
			}
			
			hbaseAdmin.createTable(desc);
			
		} catch (IOException e) {
			logger.error("IOException", e);
		}
		
	}


	public void deleteTable(String tableName) {
		
		logger.trace("deleteTable()");
		
		HBaseAdmin hbaseAdmin =  null;
		
		try {
			
			hbaseAdmin = new HBaseAdmin(configuration);
			
			if(!hbaseAdmin.tableExists(tableName)) { return; };
			
			logger.trace(String.format("Disabling table [%s].", tableName));
			hbaseAdmin.disableTable(tableName);
			
			logger.trace(String.format("Deleting table [%s].", tableName));
			hbaseAdmin.deleteTable(tableName);
			
		} catch (IOException e) {
			logger.error("IOException", e);
		}
		
	}

}
