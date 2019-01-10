package com.saikikky.HBase.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Test;


/**
 * HBase数据库操作类
 * @author saikikky
 *
 */
public class HBaseHelper {

	private static Logger logger = Logger.getLogger(HBaseHelper.class);
	
	// HBase主节点ip
	private final static String MASTER_IP = "127.0.0.1";
	// HBase表的命名空间
	private final static String HBASE_TABLE_NAMESPACE = "hbase_test";
	
	// 配置
	public static Configuration configuration = null;
	// HBase管理对象
	private HBaseAdmin hBaseAdmin = null;
	// HTable管理对象
	private HTable hTable = null;
	private ResultScanner resultScanner = null;
	// 删表锁
	@SuppressWarnings("unused")
	private boolean lockDelTable = true;
	// 删行锁
	@SuppressWarnings("unused")
	private boolean lockDelRow = true;
	
	private static final String DEFAULT_ENDODING = "UTF-8";
	
	// 静态构造函数
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", MASTER_IP);
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	/**
	 * 构造函数
	 */
	public HBaseHelper() {
		this.CreateHBaseNamespace();
	}
	
	private Connection getConnection() {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			return connection;
		} catch (IOException e) {
			logger.error("连接异常" + e.getMessage());
		}
		return null;
	}

	/**
	 * HBase的命名空间是否存在
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private boolean ExistsHBaseNamespace() {
		boolean result = false;
		try {
			this.hBaseAdmin = new HBaseAdmin(configuration);
			
			// 取现有的命名空间列表
			NamespaceDescriptor[] nds = this.hBaseAdmin.listNamespaceDescriptors();
			// 判断是否存在此命名空间
			for (NamespaceDescriptor nd : nds) {
				if (nd.getName().equals(HBASE_TABLE_NAMESPACE)) {
					result = true;
					break;
				}
			}
			logger.info(String.format("HBase Namespace Exists[%s]!", result));
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHBaseAdmin("ExistsHBaseNamespace");
		}
		return result;
	}
	
	/**
	 * 创建HBase的命名空间
	 */
	@SuppressWarnings("deprecation")
	private void CreateHBaseNamespace() {
		try {
			this.hBaseAdmin = new HBaseAdmin(configuration);
			// 不存在这个命名空间则新建
			if (!this.ExistsHBaseNamespace()) {
				NamespaceDescriptor descriptor = NamespaceDescriptor.create(HBASE_TABLE_NAMESPACE).build();
				this.hBaseAdmin.createNamespace(descriptor);
				logger.info(String.format("Create HBase Namespace[%s] success!", HBASE_TABLE_NAMESPACE));
			}
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHBaseAdmin("CreateHBaseNamespace");
		}
	}
	
	/**
	 * 删除HBase的命名空间
	 */
	@SuppressWarnings({ "deprecation", "unused" })
	private void DeleteHBaseNamespace() {
		try {
			this.hBaseAdmin = new HBaseAdmin(configuration);
			if (this.ExistsHBaseNamespace()) {
				this.hBaseAdmin.deleteNamespace(HBASE_TABLE_NAMESPACE);
				logger.info(String.format("Delete HBase Namespace[%s] success!", HBASE_TABLE_NAMESPACE));
			}
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHBaseAdmin("DeleteHBaseNamespace");
		}
	}
	
	/**
	 * 判断表是否存在
	 * @param tableName
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public boolean ExistsTable(String tableName) {
		boolean result = false;
		try {
			this.hBaseAdmin = new HBaseAdmin(configuration);
			result = this.hBaseAdmin.tableExists(tableName);
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHBaseAdmin("ExistsTable");
		}
		return result;
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param isCoverTable
	 * @param cFamilyName
	 */
	@SuppressWarnings("deprecation")
	public void CreateTable(String tableName, boolean isCoverTable, String... cFamilyName) {
		logger.info("start create table...");
		try {
			this.hBaseAdmin = new HBaseAdmin(configuration);
			tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
			// 表存在判断
			if (this.ExistsTable(tableName)) {
				// 是否强制新建表
				if (isCoverTable) {
					// 禁用表
					this.hBaseAdmin.disableTable(tableName);
					// 删除表
					this.hBaseAdmin.deleteTable(tableName);
					logger.info(tableName + " is exist, delete ....");
					// 建表准备
					TableName tn = TableName.valueOf(tableName);
					HTableDescriptor htd = new HTableDescriptor(tn);
					// 列族
					for (int i = 0; i < cFamilyName.length; i++) {
						HColumnDescriptor hcd = new HColumnDescriptor(cFamilyName[i]);
						htd.addFamily(hcd);
					}
					// 建表
					this.hBaseAdmin.createTable(htd);
					
					logger.info("Cover HBase Table[" + tableName + "] success!");
				} else {
					logger.info(tableName + " is exist ... no create table!");
				}
			} else {
				// 建表准备
				TableName tn = TableName.valueOf(tableName);
				HTableDescriptor htd = new HTableDescriptor(tn);
				// 列族
				for (int i = 0; i < cFamilyName.length; i++) {
					HColumnDescriptor hcd = new HColumnDescriptor(cFamilyName[i]);
					htd.addFamily(hcd);
				}
				
				// 建表
				this.hBaseAdmin.createTable(htd);
				
				logger.info("Create New HBase Table[" + tableName + "] success!");
			}
		} catch (MasterNotRunningException e) {
			logger.error(e.getMessage());
		} catch (ZooKeeperConnectionException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHBaseAdmin("CreateTable");
		}
		logger.info("end create table ....");
	}
	
	/**
	 * 添加一行多限定符的数据
	 * @param tableName 表名
	 * @param columnFamily 列族
	 * @param rowKey 行键
	 * @param cqAndValue 列名(列限定符)和值的键值对
	 */
	@SuppressWarnings("deprecation")
	public void AddRowData(String tableName, String columnFamily, String rowKey, Map<String, String> cqAndValue) {
		if (cqAndValue.isEmpty()) return;
		tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
		try {
			List<Put> puts = new ArrayList<Put>();
			// keyset()返回所有key值列表
			for (String cq : cqAndValue.keySet()) {
				hTable = new HTable(configuration, tableName);
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(cq), Bytes.toBytes(cqAndValue.get(cq).toString()));
				puts.add(put);
			}
			hTable.put(puts);
		} catch (IOException e) {
			logger.error(e.getMessage());
		} finally {
			this.CloseHTableAdmin("AddRowData");
		}
	}
	
	/**
	 * 插入 AddRowData和insertData用哪个还需要测试
	 * @param tableName
	 * @param columnFamily
	 * @param dataMap
	 */
	@SuppressWarnings("deprecation")
	public void insertData(String tableName, String columnFamily, Map<Integer, Map<String, String>> dataMap) {
		
		try {
			tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
			List<Put> resultPuts = new ArrayList<Put>();
			this.hTable = new HTable(configuration, tableName);
			for (Entry<Integer, Map<String, String>> dataEntry : dataMap.entrySet()) {
				Integer rowKey = dataEntry.getKey(); // 行键为行数(唯一标识)
				Put put = new Put(Bytes.toBytes(rowKey));
				Map<String, String> childMap = dataEntry.getValue();
				for (Entry<String, String> childEntry : childMap.entrySet()) {
					String columnName = childEntry.getKey();
					Object columnValue = childEntry.getValue();
					put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue.toString()));
				}
				resultPuts.add(put);
			}
			this.hTable.put(resultPuts);
			this.CloseHBaseAdmin("insertData for table : " + tableName);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}/*
		try {
			   tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
			   //TableName tablename = TableName.valueOf(tableName);
			   List<Put> resultPuts = new ArrayList<Put>();
			   this.hTable = new HTable(configuration, tableName);
			   for(Entry<Integer, Map<String , String>> dataEntry: dataMap.entrySet()) {
			    Integer rowKey = dataEntry.getKey();
			    Put put = new Put(rowKey.toString().getBytes(DEFAULT_ENDODING));
			    Map<String , String > childMap = dataEntry.getValue();
			    for(Entry<String , String> childEntry :childMap.entrySet()) {
			     String columnName = childEntry.getKey();
			     String columnValue = childEntry.getValue();
			     //参数：1.列族名 2.列名 3.列值
			     put.addColumn(columnFamily.getBytes(DEFAULT_ENDODING), columnName.getBytes(DEFAULT_ENDODING),
			       columnValue.getBytes(DEFAULT_ENDODING));
			    }
			    resultPuts.add(put);
			   }
			   this.hTable.put(resultPuts);
			   this.CloseHBaseAdmin("insertData for table : " + tableName);
			  } catch (IOException e) {
			   logger.error(e);
			   e.printStackTrace();
			  }*/
	}
	
	/**
	 * 导出hbase数据
	 * @param tableName
	 * @return
	 */
	public Map<Integer, Map<String, String>> exportData(String tableName) {
		
		Map<Integer, Map<String, String>> resultMap = new HashMap<Integer, Map<String,String>>();
		tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
		// 获取TableName
		TableName tablename = TableName.valueOf(tableName);
		
		try	{
			this.hTable = new HTable(configuration, tableName);
			Table table = getConnection().getTable(tablename);
			Scan scan = new Scan();
			ResultScanner resultScanner = table.getScanner(scan);
			
			Map<String, String> cellMap = null;
			// 遍历ResultScanner
			for (Result result : resultScanner) {
				cellMap = new HashMap<String, String>();
				Integer rowKey = null;
				for (Cell cell : result.rawCells()) {
					// 获取rowKey
					//rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					rowKey = Bytes.toInt(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					System.out.println(rowKey);
					// 获得列名
					String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
					String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
					cellMap.put(colName, columnValue);
					
				}
				resultMap.put(rowKey, cellMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultMap;
		
		/*
		Map<Integer, Map<String, String>> resultMap = new HashMap<Integer, Map<String, String>>();
		  tableName = HBASE_TABLE_NAMESPACE + ":" + tableName;
		  //获取TableName
		  TableName tablename = TableName.valueOf(tableName);
		  
		  try {
		   this.hTable = new HTable(configuration, tableName);
		   Table table = getConnection().getTable(tablename);
		   Scan scan = new Scan();
		   ResultScanner resultScanner = table.getScanner(scan);
		   
		   Map<String , String > cellMap = null;
		   //遍历ResultScanner
		   for(Result result : resultScanner) {
		    cellMap = new HashMap<String, String>();
		    String rowKey = null;
		    for(Cell cell : result.rawCells()) {
		     //获得rowKey
		     rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
		     //获得列名
		     String colName = Bytes.toString(cell.getQualifierArray(),
		       cell.getQualifierOffset(),cell.getQualifierLength());
		     
		     String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
		     cellMap.put(colName, columnValue);
		     
		    }
		    resultMap.put(Integer.parseInt(rowKey), cellMap);
		   }
		  } catch (IOException e) {
		   e.printStackTrace();
		  }
		   return resultMap;*/
	}
	
	/**
	 * 关闭HBase连接
	 * @param methodName
	 */
	private void CloseHBaseAdmin(String methodName) {
		try {
			this.hBaseAdmin.close();
			logger.info(methodName + "(...):关闭与HBase的连接！");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * 关闭HTable连接
	 * @param methodName
	 */
	private void CloseHTableAdmin(String methodName) {
		try {
			this.hTable.close();
			logger.info(methodName + "(...):关闭与HTable的连接！");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	/**
	 * 关闭扫描器
	 * @param methodName
	 */
	@SuppressWarnings("unused")
	private void CloseResultScanner(String methodName) {
		this.resultScanner.close();
		logger.info(methodName + "(...):关闭与ResultScanner的连接!");
	}
	
	@Test
	public void testCreateHBaseNamespace() {
		CreateHBaseNamespace();
	}
	
	@Test
	public void testCreateTable() {
		CreateTable("Student", true, "stuInfo");
	}
	
	@Test
	public void testAddRowData() {
		Map<String, String> cqs = new HashMap<String, String>();
		cqs.put("id", "1");
		cqs.put("name", "Tommy");
		AddRowData("Student", "stuInfo", "row1", cqs);
	}
	
	@Test
	public void testinsertData() {
		Map<Integer, Map<String, String>> dataMap = new HashMap<Integer, Map<String,String>>();
		Map<String, String> map = new HashMap<String, String>();
		map.put("id", "2");
		map.put("name", "Tommy");
		
		
		dataMap.put(2, map);
		insertData("Student", "stuInfo", dataMap);
	}
}
