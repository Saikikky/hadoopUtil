package HDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListFiles {
	public static void main(String[] agrs) {
		String uri = "";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(new URI(uri), conf);
			Path path = new Path(uri);
			FileStatus status[] = fs.listStatus(path);
			for (int i = 0; i < status.length; i++) {
				System.out.println(status[i].getPath().toString());
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
