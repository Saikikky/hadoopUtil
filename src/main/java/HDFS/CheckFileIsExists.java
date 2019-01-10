package HDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 查看问价是否存在
 * @author saikikky
 *
 */
public class CheckFileIsExists {
	public static void main(String[] agrs) {
		String uri = ""; // 可以指定目录或者指定文件
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(new URI(uri), conf);
			Path path = new Path(uri);
			boolean isExists = fs.exists(path);
			String str = isExists ? "Exists" : "Not Exists";
			System.out.println("指定文件或目录" + str);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
