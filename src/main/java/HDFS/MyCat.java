package HDFS;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用java.net.URL访问HDFS文件系统
 * 显示HDFS文件夹中的文件内容
 * 使用java.net.URL对象打开数据流
 * 使用静态代码块使得java程序识别Hadoop的HDFS url
 * @author saikikky
 *
 */

public class MyCat {
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		InputStream input = null;
		try {
			input = new URL(args[0]).openStream();
			IOUtils.copyBytes(input, System.out,4098,false);
		} catch (Exception e) {
			System.out.println("Error");
		} finally {
			IOUtils.closeStream(input);
		}
	}
	
}
