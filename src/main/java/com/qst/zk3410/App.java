package com.qst.zk3410;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * Hello world!
 *
 */
public class App {
	/**
	 * get数据
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		Stat st = new Stat();
		ZooKeeper zk = new ZooKeeper("s201:2181", 5000000, null);
		byte[] b = zk.getData("/a", false, st);
		System.out.println();
		System.out.println(new String(b));
	}

	/**
	 * set数据
	 * 
	 * @param args
	 * @throws Exception
	 */
	@Test
	public void tsetSet() throws Exception {

		ZooKeeper zk = new ZooKeeper("s201:2181", 5000000, null);
		// version必须匹配，-1为通配
		Stat st = zk.setData("/a", "jerry".getBytes(), 2);
		System.out.println(st);
	}

	/**
	 * create数据
	 * 
	 * @param args
	 * @throws Exception
	 */
	@Test
	public void tsetCreate() throws Exception {

		ZooKeeper zk = new ZooKeeper("s202:2181", 5000, null);
		// version必须匹配，-1为通配
		FileInputStream fis = new FileInputStream("E:/1.txt");
		byte[] b = new byte[fis.available()];
		fis.read(b);
		fis.close();
		String st = zk.create("/flume/a1", b, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(st);
	}

	/**
	 * 递归列出孩子节点
	 * 
	 * @param args
	 * @throws Exception
	 */
	@Test
	public void listAll() throws Exception {

		ZooKeeper zk = new ZooKeeper("s203:2181", 5000, null);
		listChildren(zk, "/");
	}

	public void listChildren(ZooKeeper zk, String path) throws Exception {

		System.out.println(new String(zk.getData(path , false, new Stat())));
		// version必须匹配，-1为通配
		List<String> list = zk.getChildren(path, false);
		for (String child : list) {
			if (path.equals("/")) {
				path = "";
			}
			// /ab0000000000
			System.out.println(path + "/" + child);
			path = path + "/" + child;
			listChildren(zk, path);
			
		}
	}

	/**
	 * 观察者模式
	 * @throws Exception
	 */
	@Test
	public void watch() throws Exception {
		
		final ZooKeeper zk = new ZooKeeper("s201:2181", 5000, null);
		Watcher w = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event);
				if(event.getType() == EventType.NodeDeleted){
					System.exit(0);
				}
				System.out.println(event.getPath() + ":出事了！" + event.getType());
				try {
					zk.getData("/a", this, new Stat());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		Stat st = new Stat();
		byte[] b = zk.getData("/a", w, st);
		//System.out.println(new String(b));
		while (true) {
			Thread.sleep(1000);
		}
		// System.out.println(st);
	}

}
