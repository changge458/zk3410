package com.qst.zk3410;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ListChild {

	public static void main(String[] args) throws Exception  {
		ZooKeeper zk = new ZooKeeper("s203:2181", 5000, null);
		listChildren(zk, "/");
	}

	public static void listChildren(ZooKeeper zk, String path)  {
		if(path.equals("/hbase") || path.equals("/yarn-leader-election") || path.equals("/zookeeper") || path.equals("/hadoop-ha") ){
			return;
		}
		try {
			String data = getData(zk,path);
			System.out.println(path+ "===========" + data);
			
			List<String> child = zk.getChildren(path, false);
			if(child == null || child.isEmpty() ){
				return;
			}
			for(String str : child){
				if(path.equals("/")){
					path = "";
				}
				
				listChildren(zk, path+"/"+str);
			}
		} catch (Exception e) {
		}
	}

	private static String getData(ZooKeeper zk, String path) {
		try {
			Stat st = new Stat();
			byte[] b;
			b = zk.getData(path, false, st);
			return new String(b);
		} catch (Exception e) {
		}
		return null;
	}
}
