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

 package com.taobao.adfs.benchmark;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadThread implements Runnable {
	
	private FileSystem fs = null;
	private String dirToRead;
	private CountDownLatch fire;
	private CountDownLatch over;
	private int threadIndex;
	private Output output;
	public ReadThread(int threadIndex, FileSystem fs, CountDownLatch fire, 
								CountDownLatch over, Output output, String dirToRead)
	{
		this.threadIndex = threadIndex;
		this.fs = fs;
		this.fire = fire;
		this.over = over;
		this.dirToRead = dirToRead;
		this.output = output;
	}
	@Override
	public void run() {
		FSDataInputStream in = null;
		String computerName = "unknown";
		InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("can not get host name");
		}
		if(addr != null)
			computerName = addr.getHostName(); 
		Path createPath = new Path(dirToRead);
		Path basePath = new Path(createPath, computerName + "/" + this.threadIndex);
		try {
			if(!fs.exists(basePath ))
			{
				System.out.println(dirToRead + " dir does not exist");
				System.out.println("please run creat_write first or create one dir including some files");
				System.exit(1);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("fs.exists() error!");
		}
		try {
			fire.await();
			readFiles(basePath, in);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			over.countDown();
			try {
				if(in!=null)
					in.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void readFiles(Path createPath, FSDataInputStream in) throws IOException {
		FileStatus[] status = fs.listStatus(createPath);
		if(status==null||status.length==0)
			return;
		for(int i=0;i<status.length;i++)
		{
			if(status[i].isDir())
				readFiles(status[i].getPath(), in);
			else
			{
				in = fs.open(status[i].getPath());
				readData(in, status[i].getLen());
				in.close();
				output.recordTime(System.currentTimeMillis());
			}
		}
	}
	private void readData(FSDataInputStream in, long len) throws IOException {
		byte[] buffer = new byte[1024];
		long actualSize = 0;
		while(actualSize!=len)
		{
			actualSize+=in.read(buffer, 0, buffer.length);
		}
	}
}
