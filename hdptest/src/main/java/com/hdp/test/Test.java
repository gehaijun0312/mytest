package com.hdp.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class Test {
	public static void main(String[] args) throws Exception{
		/* String uri = "hdfs://9.111.254.189:9000/";  
	        Configuration config = new Configuration();  
	        FileSystem fs = FileSystem.get(URI.create(uri), config);  
	   
	        // 列出hdfs上/user/fkong/目录下的所有文件和目录  
	        FileStatus[] statuses = fs.listStatus(new Path("/user/fkong"));  
	        for (FileStatus status : statuses) {  
	            System.out.println(status);  
	        }  
	   
	        // 在hdfs的/user/fkong目录下创建一个文件，并写入一行文本  
	        FSDataOutputStream os = fs.create(new Path("/user/fkong/test.log"));  
	        os.write("Hello World!".getBytes());  
	        os.flush();  
	        os.close();  
	   
	        // 显示在hdfs的/user/fkong下指定文件的内容  
	        InputStream is = fs.open(new Path("/user/fkong/test.log"));  
	        IOUtils.copyBytes(is, System.out, 1024, true);  */
		
		
		try {

			String localSrc = "D://hello.txt";

			String dst = "hdfs://192.168.175.128:9000/user/root/hello.txt";

			InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

			Configuration conf = new Configuration();

			 

			FileSystem fs = FileSystem.get(URI.create(dst), conf);

			OutputStream out = fs.create(new Path(dst), new Progressable() {

			public void progress() {

			System.out.print(".");

			}

			});

			IOUtils.copyBytes(in, out, 4096, true);

			System.out.println("success");

			} catch (Exception e) {
				e.printStackTrace();
			}

	}
}
