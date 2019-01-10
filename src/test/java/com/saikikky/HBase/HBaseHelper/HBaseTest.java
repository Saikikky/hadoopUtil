package com.saikikky.HBase.HBaseHelper;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Test;

import Common.ExcelPOIHelper;

public class HBaseTest {

	@Test
	public void HBaseExcelTest() {
		Map<Integer, Map<String, String>> map = ExcelPOIHelper.GetExcelData("/Users/saikikky/Documents/实训/项目相关/Java.xlsx", "Java");
		
		HBaseHelper hBase = new HBaseHelper();
		
		String tableName = new String("position_info");
		
		String columnFamily = new String("job_info");
		
		hBase.CreateTable(tableName, true, columnFamily);
		
		hBase.insertData(tableName, columnFamily, map);
	}
	
	@Test
	public void HBaseToExcelTest() {
		HBaseHelper hbase = new HBaseHelper();
		
		Map<Integer, Map<String, String>> map = hbase.exportData("position_info");
		
		ExcelPOIHelper.Create("/Users/saikikky/Documents/实训/项目相关/JavaExport.xlsx", "Java", map);
	}

	@Test
	public void HBase2HDFSTest() {
		String hdfsFilePath = "hdfs://localhost:9000/hbase";
		String hdfsFileName = "JAVA";
		String description = "exportFrom table : position_info to hdfs";
		HBase2HDFSHelper.transferFromHBase2HDFS("position_info", hdfsFilePath, hdfsFileName, description);
	}
}
