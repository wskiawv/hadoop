package com.core.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HadoopUtil {
	private FileSystem fs=null;
	
	@Before
	public void getFs(){
		try{			
			Configuration conf =new Configuration();
			conf.set("fs.defaultFS", "hdfs://192.168.0.201:9000");
			conf.set("dfs.replication", "1");
			fs=FileSystem.get(conf);
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	@Test
	public void upload(){
		try{
			fs.copyFromLocalFile(new Path("e:/我的资料/基于hadoop数据仓库.ppt"),new Path("hdfs://192.168.0.201:9000/"));
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	@Test
	public void download(){
		try{
			fs.copyToLocalFile(new Path("hdfs://192.168.0.201:9000/基于hadoop数据仓库.ppt"), new Path("d:/基于hadoop数据仓库.ppt"));
			
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	@Test
	public void delete(){
		try{
			Boolean s=fs.delete(new Path("/aa/bb"),true);
			System.out.println(s?"delete success":"delete fail");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	@Test
	public void mkdir(){
		try{
			fs.mkdirs(new Path("/aa/bb"));
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	@Test
	public void rename(){
		try{
			fs.rename(new Path("/aa/bb"), new Path("/aa/cc"));
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
