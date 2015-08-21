package com.jorgefigueiredo.hbase.repository.tests;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.jorgefigueiredo.hbase.repository.ITableRepository;
import com.jorgefigueiredo.hbase.repository.TableRepository;

public class TableRepositoryTests {

	@Test
	public void testTableExists() throws IOException {
		
		ITableRepository tableRepository = new TableRepository();
		
		assertTrue(tableRepository.containsTable("test"));
		assertFalse(tableRepository.containsTable("test2"));
		
	}

	
	@Test
	public void testCreateTable() throws IOException {
		
		ITableRepository tableRepository = new TableRepository();
		
		String tableName = "xxx";
		
		assertFalse(tableRepository.containsTable(tableName));
		
		tableRepository.createTable(tableName, new String[] { "cf" });
		
		assertTrue(tableRepository.containsTable(tableName));
		
	}
	
	@Test
	public void testDeleteTable() throws IOException {
		
		ITableRepository tableRepository = new TableRepository();
		
		String tableName = "xxx";
		
		if(!tableRepository.containsTable(tableName)) {
			tableRepository.createTable(tableName, new String[] { "cf" });
		}
		
		assertTrue(tableRepository.containsTable(tableName));
		
		tableRepository.deleteTable(tableName);
		
		assertFalse(tableRepository.containsTable(tableName));
		
	}
	
}
