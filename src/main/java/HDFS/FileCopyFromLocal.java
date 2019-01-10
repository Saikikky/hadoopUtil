package HDFS;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



public class FileCopyFromLocal {

	public static void main(String[] args) throws Exception {
		String source = " ";
		String destination = " ";
		
		InputStream in = new BufferedInputStream(new FileInputStream(source));
		// HDFS读写的配置文件
		Configuration conf = new Configuration();
		// 调用Filesystem的create方法返回的是FSDataOutputStream对象
		// 该对象不允许在文件中定位，因为HDFS只允许一个已打开的文件顺序写入或追加
		FileSystem fs = FileSystem.get(URI.create(destination), conf);
		OutputStream out = fs.create(new Path(destination));
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
