package HDFS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;


import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtil {
	//这里的path写成hadoop中core-site.xml中的fs.default.nameֵ
	private static final String DEFAULT_HDFS_PATH = "hdfs://localhost:19000";
	
	private String hdfsPath;
	
	private Configuration conf;

	public HDFSUtil(String hdfsPath, Configuration conf) {
		this.hdfsPath = hdfsPath;
		this.conf = conf;
	}
	
	public HDFSUtil( Configuration conf) {
		this.hdfsPath = DEFAULT_HDFS_PATH;
		this.conf = conf;
	}
	
	public static JobConf config() {
		JobConf jobConf = new JobConf();
		jobConf.setJobName("hdfsDao");
		return jobConf;
	}
	
	private FileSystem getFileSystem() throws IOException {
		return FileSystem.get(URI.create(hdfsPath), conf);
	}
	
	/**
	 * 创建目录
	 * @param folder
	 * @throws IOException
	 */
	public void mkdir(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = getFileSystem();
		if(! fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create directory:" + folder);
		}
		fs.close();
	}
	
	/**
	 * 删除目录或文件
	 * @param folder
	 * @throws IOException
	 */
	public void rm(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = getFileSystem();
		fs.deleteOnExit(path);
		System.out.println("Delete file or directory: " + folder);
		fs.close();
	}
	
	/**
	 *遍历目录文件
	 * @param folder
	 * @throws IOException
	 */
	public void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = getFileSystem();
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls : " + folder);
		System.out.println("========================================");
		for(FileStatus status : list) {
			System.out.printf("name: %s, folder : %s , size : %d\n", status.getPath(), status.isDir(), status.getLen());
			
		}
		System.out.println("========================================");
		fs.close();
	}
	
	/**
	 * 创建文件
	 * @param filename
	 * @param content
	 * @throws IOException
	 */
	public void createFile(String filename , String content) throws IOException {
		FileSystem fs = getFileSystem();
		byte[] buffer = content.getBytes();
		FSDataOutputStream fsos = null;
		try {
			fsos = fs.create(new Path(filename));
			fsos.write(buffer, 0, buffer.length);
			System.out.println("Create new File: " + filename);
		} finally {
			if(null != fsos) {
				fsos.close();
			}
		}
		fs.close();
		
	}
	
	/**
	 * 将本地文件复制到HDFS
	 * @param localPath
	 * @param hdfsPath
	 * @throws IOException
	 */
	public void copyFromLocalToHDFS(String localPath, String hdfsPath) throws IOException {
		FileSystem fs = getFileSystem();
		fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
		System.out.println("copy from localPath:" + localPath + " to HDFSPath: "+ hdfsPath);
		fs.close();
	}
	
	/**
	 * 从HDFS目录下载文件到本地
	 * @param hdfsPath
	 * @param localPath
	 * @throws IOException
	 */
	public void downloadFromHDFS(String hdfsPath, String localPath) throws IOException {
		Path path = new Path(hdfsPath);
		FileSystem fs = getFileSystem();
		fs.copyToLocalFile(path, new Path(localPath));
		System.out.println("download file from HDFSPath : " + hdfsPath + " to localPath : " + localPath);
		fs.close();
	}
	
	/**
	 * 查看文件内容
	 * @param hdfsPath
	 * @return
	 * @throws IOException
	 */
	public String cat(String hdfsPath) throws IOException {
		Path path = new Path(hdfsPath);
		FileSystem fs = getFileSystem();
		FSDataInputStream fsis = null;
		System.out.println("cat : "+ hdfsPath);
		
		OutputStream baos = new ByteArrayOutputStream();
		String str = null;
		try {
			fsis = fs.open(path);
			IOUtils.copyBytes(fsis, baos, 4096, false);
			str = baos.toString();
		} finally {
			IOUtils.closeStream(fsis);
			fs.close();
		}
		System.out.println(str);
		
		return str;
	}
	
	/**
	 * 返回给定文件的位置
	 * @throws IOException
	 */
	public void location() throws IOException {
		String folder  = hdfsPath + "/";
		String file = "sample.txt";
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration());
		
		FileStatus status = fs.getFileStatus(new Path(folder + file));
		BlockLocation[] list = fs.getFileBlockLocations(status, 0, status.getLen());
		
		System.out.println("File Location : " + folder +file);
		for(BlockLocation bl : list) {
			String [] hosts = bl.getHosts();
			for(String host : hosts){
				System.out.println("host :" + host);
			}
		}
		fs.close();
		
	}
	
	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = config();
		HDFSUtil hdfs = new HDFSUtil(conf);
		
		//hdfs.copyFromLocalToHDFS("D:\\test\\logs.txt", "/wordCount/logs");
		
		//hdfs.cat("/wordCount/logs");
		
		//hdfs.downloadFromHDFS("/wordCount/logs", "D:\\test\\logs1.txt");
		
		//hdfs.mkdir("/newDir");
		
		hdfs.ls("/wordCount");
	}
	
	
}
