package HDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {

	public static void main(String[] agrs) {
		String uri="";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(new URI(uri), conf);
			Path f = new Path(uri);
			// 递归删除文件夹下所有文件
			boolean isDelete = fs.delete(f, true);
			// boolean isDelete = fs.delete(f, false);
			String str = isDelete ? "Success" : "Error";
			System.out.println("删除" + str);
		} catch (Exception e) {
			System.out.println("删除出错");
		}
	}
}
