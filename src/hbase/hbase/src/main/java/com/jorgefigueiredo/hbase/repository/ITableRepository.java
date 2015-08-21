package com.jorgefigueiredo.hbase.repository;



public interface ITableRepository {

	boolean containsTable(String tableName);
	void deleteTable(String tableName);
	
	void createTable(String tableName, String[] columnFamilies);
	
}
