/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.net.NetUtils;
/**
 * http://<nn>:<port>/listPaths[/<path>][<?option>[&option]*]
 * Where option in:
   * recursive (yes|no)
   * filter 
   * exclude
 * 
 */
public class TestMNListPaths extends TestCase {
	
	private String runListPaths(MiniMNDFSCluster cluster, Configuration conf, String path, boolean recursive, String exclude, String filter)
	{
		
		String xmlString = null;
		BufferedReader input = null;
		ByteArrayOutputStream bStream = null;
		try {
			String fsName = NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                    "dfs.info.port", "dfs.http.address");
			StringBuffer url = new StringBuffer("http://"+fsName+"/listPaths");
			if(path!=null&&path!="")
				url.append(URLEncoder.encode(path, "UTF-8"));
			if(recursive||(exclude!=null&&exclude!="")||(filter!=null&&filter!=""))
			{
				boolean haveParmeter = false;
				url.append("?");
				if(recursive)
				{
					url.append("recursive=yes");
					haveParmeter = true;
				}
				if(exclude!=null&&exclude!="")
				{
					if(haveParmeter)
						url.append("&");
					url.append("exclude="+exclude);
					haveParmeter = true;
				}
				if(filter!=null&&filter!="")
				{
					if(haveParmeter)
						url.append("&");
					url.append("filter="+filter);
					haveParmeter = true;
				}
			}
			URL pathurl = new URL(url.toString());
		    URLConnection connection = pathurl.openConnection();
		    InputStream stream = connection.getInputStream();
		    PrintStream oldOut = System.out;
		    bStream = new ByteArrayOutputStream();
		    PrintStream newOut = new PrintStream(bStream, true);
		    System.setOut(newOut);
		    input = new BufferedReader(new InputStreamReader(
		                                              stream, "UTF-8"));
		    String line = null;
	        while ((line = input.readLine()) != null) {
	        	System.out.println(line);
	        }
	        xmlString = bStream.toString();
	        System.setOut(oldOut);
	        return xmlString;
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			try {
				if(input != null)
					input.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return xmlString;
	}
	/*
	 * test ListPathsServlet function without any parameter
	 */
	public void testListPathsWithoutParameter()
	{
		MiniMNDFSCluster cluster = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.setLong("dfs.blockreport.intervalMsec", 10000L);
		URL resource = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
		conf.addResource(resource);
		conf.set("dfs.namenode.port.list", "0,0");
		String pathStr = "/test";
		try {
			cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
			cluster.waitDatanodeDie();
			fs = cluster.getFileSystem(1);
			DFSTestUtil.createFile(fs, new Path(pathStr), 2048, (short)3, 0);
			String xml = runListPaths(cluster,conf,null,false,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(pathStr));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cluster.shutdown();
		}
	}
	
	/*
	 * test ListPathsServlet function with "recursive" parameter
	 */
	public void testListPathsWithRecursive()
	{
		MiniMNDFSCluster cluster = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.setLong("dfs.blockreport.intervalMsec", 10000L);
		URL resource = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
		conf.addResource(resource);
		conf.set("dfs.namenode.port.list", "0,0");
		String pathStr = "/test/test";
		try {
			cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
			cluster.waitDatanodeDie();
			fs = cluster.getFileSystem(1);
			DFSTestUtil.createFile(fs, new Path(pathStr), 2048, (short)3, 0);
			String xml = runListPaths(cluster,conf,null,false,null,null);
			System.out.println(xml);
			assertFalse(xml.contains(pathStr));
			xml = runListPaths(cluster,conf,null,true,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(pathStr));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cluster.shutdown();
		}
	}
	
	/*
	 * test ListPathsServlet function with "filter" parameter
	 */
	public void testListPathsWithFilter()
	{
		MiniMNDFSCluster cluster = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.setLong("dfs.blockreport.intervalMsec", 10000L);
		URL resource = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
		conf.addResource(resource);
		conf.set("dfs.namenode.port.list", "0,0");
		String test = "/test";
		String abc = "/abc";
		
		try {
			cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
			cluster.waitDatanodeDie();
			fs = cluster.getFileSystem(1);
			DFSTestUtil.createFile(fs, new Path(test), 1024, (short)3, 0);
			DFSTestUtil.createFile(fs, new Path(abc), 1024, (short)3, 0);
			String xml = runListPaths(cluster,conf,null,true,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(test));
			assertTrue(xml.contains(abc));
			xml = runListPaths(cluster,conf,null,true,null,"test");
			System.out.println(xml);
			assertTrue(xml.contains(test));
			assertFalse(xml.contains(abc));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cluster.shutdown();
		}
	}
	
	/*
	 * test ListPathsServlet function with "exclude" parameter
	 */
	public void testListPathsWithExclude()
	{
		MiniMNDFSCluster cluster = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.setLong("dfs.blockreport.intervalMsec", 10000L);
		URL resource = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
		conf.addResource(resource);
		conf.set("dfs.namenode.port.list", "0,0");
		String test = "/test";
		String abc = "/abc";
		
		try {
			cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
			cluster.waitDatanodeDie();
			fs = cluster.getFileSystem(1);
			DFSTestUtil.createFile(fs, new Path(test), 1024, (short)3, 0);
			DFSTestUtil.createFile(fs, new Path(abc), 1024, (short)3, 0);
			String xml = runListPaths(cluster,conf,null,true,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(test));
			assertTrue(xml.contains(abc));
			xml = runListPaths(cluster,conf,null,true,"test",null);
			System.out.println(xml);
			assertFalse(xml.contains(test));
			assertTrue(xml.contains(abc));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cluster.shutdown();
		}
	}
	
	/*
	 * test ListPathsServlet function with "path"
	 */
	public void testListPathsWithPath()
	{
		MiniMNDFSCluster cluster = null;
		FileSystem fs = null;
		Configuration conf = new Configuration();
		conf.setLong("dfs.blockreport.intervalMsec", 10000L);
		URL resource = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
		conf.addResource(resource);
		conf.set("dfs.namenode.port.list", "0,0");
		String test = "/test";
		String abc = "/abc";
		
		try {
			cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
			cluster.waitDatanodeDie();
			fs = cluster.getFileSystem(1);
			DFSTestUtil.createFile(fs, new Path(test), 1024, (short)3, 0);
			DFSTestUtil.createFile(fs, new Path(abc), 1024, (short)3, 0);
			String xml = runListPaths(cluster,conf,null,true,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(test));
			assertTrue(xml.contains(abc));
			xml = runListPaths(cluster,conf,test,true,null,null);
			System.out.println(xml);
			assertTrue(xml.contains(test));
			assertFalse(xml.contains(abc));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			cluster.shutdown();
		}
	}
}
