package HDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 创建HDFS目录
 * @author saikikky
 *
 */
public class CreateDirction {

	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(new URI(uri), conf);
			Path dfs = new Path(uri);
			fs.mkdirs(dfs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("SUCCESS");
		}
	}
}
