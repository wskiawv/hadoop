package com.core.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HadoopTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		getFs();
		upload();
	}
	private static FileSystem fs=null;
	
	//@Before
	public static void getFs(){
		try{			
			Configuration conf =new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.0.201:9000");
			conf.set("dfs.replication", "1");
			fs=FileSystem.get(conf);
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	//@Test
	public static void upload(){
		try{
			fs.copyFromLocalFile(new Path("e:/我的资料/基于hadoop数据仓库.ppt"),new Path("hdfs://192.168.0.201:9000/"));
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}

}
