package HDFS;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author saikikky
 * 查看文件存储位置
 */

public class LocationFIle {

	public static void main(String[] args) {
		String uri = "";
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(new URI(uri), conf);
			Path path = new Path(uri);
			FileStatus fileStatus = fs.getFileStatus(path);
			BlockLocation blkLocation[] = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for (int i = 0; i < blkLocation.length; i++) {
				String[] hosts = blkLocation[i].getHosts();
				System.out.println("block_" + i + "_Location:" + hosts[0]);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
