package com.peng;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.8.101:9000"), conf, "root");
		fileSystem.copyFromLocalFile(new Path("c:\\rest.txt"), new Path("/user/rest.txt"));
		fileSystem.close();
		System.out.println("上传成功");
	}
}
