package com.saikikky.HBase.HBaseHelper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 把HBase数据导入HDFS
 * @author saikikky
 *
 */
public class HBase2HDFSHelper {
	private static Configuration configuration = HBaseConfiguration.create();
	
	// HBase表的命名空间
	private final static String HBASE_TABLE_NAMESPACE = "hbase_test";
	
	static {
		// 设置zookeeper
		configuration.set("hbase.zookeeper.quorum", "localhost");
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
	}
	
	private static class TableMap extends TableMapper<Text, Text> {
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) 
			throws IOException, InterruptedException {
			// key就是hbase的rowKey
			// String rowKey;
			Integer rowKey;
			StringBuilder sb = new StringBuilder();
			for (Cell cell : value.rawCells()) {
				// 获得rowKey
				// rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
				rowKey = Bytes.toInt(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
				// 获得列名
				String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
				String columnValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
				sb.append(colName).append(":").append(columnValue).append("\t");
			}
			context.write(new Text(key.get()), new Text(sb.toString()));
		}
	}
	
	private static class HDFSReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();
		
		protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, val);
			}
		}
	}
	
	/**
	 * 传递hbase数据至hdfs
	 * @param tableName
	 * @param hdfsFilePath
	 * @param hdfsFileName
	 * @param description job描述
	 */
	public static void transferFromHBase2HDFS(String tableName, String hdfsFilePath, String hdfsFileName, String description) {
		createJob(tableName, hdfsFilePath, hdfsFileName, description);
	}
	
	/**
	 * 初始化Job
	 * @param tableName
	 * @param hdfsFilePath
	 * @param hdfsFileName
	 * @param description
	 */
	private static void createJob(String tableName, String hdfsFilePath, String hdfsFileName, String description) {
		try {
			Job job = Job.getInstance(configuration, description);
			job.setJarByClass(HBase2HDFSHelper.class);
			// 创建对hbase的扫描类
			Scan scan = new Scan();
			// 选择相应的hbase表作为map输入
			TableMapReduceUtil.initTableMapperJob(HBASE_TABLE_NAMESPACE + ":" + tableName, scan, TableMap.class, Text.class, Text.class, job);
			String realHDFSFilePath = hdfsFilePath + "/" + hdfsFileName;
			Path outPath = new Path(realHDFSFilePath);
			FileSystem.get(configuration).delete(outPath, true);
			FileOutputFormat.setOutputPath(job, outPath);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
