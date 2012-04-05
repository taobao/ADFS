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
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriterThread implements Runnable {
	
	private int threadIndex;
	private FileSystem fs = null;
	private long fileSize =0;
	private long blockSize = 64*1024*1024;
	private int fileNum = 0;
	private CountDownLatch fire;
	private CountDownLatch over;
	private Output output;
	
	public void setOutput(Output output)
	{
		this.output = output;
	}
	
	public Output getOutput()
	{
		return this.output;
	}
	
	public WriterThread(int threadIndex, FileSystem fs, long fileSize, long blockSize, int fileNum,
												CountDownLatch fire, CountDownLatch over,Output output)
	{
		this.threadIndex = threadIndex;
		this.fs = fs;
		this.fileSize = fileSize;
		this.blockSize = fs.getConf().getLong("dfs.block.size",blockSize);
		this.fileNum = fileNum;
		this.fire = fire;
		this.over = over;
		this.output = output;
	}
	@Override
	public void run() {
		FSDataOutputStream out = null;
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
		Path createPath = new Path("/create_write");
		Path basePath = new Path(createPath, computerName + "/" + this.threadIndex);
		try {
			if(fs.exists(createPath ))
				fs.delete(createPath, true);
		} catch (IOException e1) {
			System.out.println("delete " + createPath.toString() + " error!!!");
			e1.printStackTrace();
		}
		try {
			fire.await();
			for(int i = 0; i< fileNum; i++ )
			{
				out = fs.create(new Path(basePath, "file_" +i ), false, 512, (short)3, blockSize);
				writeData(out, fileSize);
				out.close();
				output.recordTime(System.currentTimeMillis());
//				printer.println(System.currentTimeMillis());
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			over.countDown();
			try {
				if(out!=null)
					out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private void writeData(FSDataOutputStream out, long fileSize) {
		byte[] buffer = new byte[512];
		new Random().nextBytes(buffer);
		long remainded = fileSize;
		while(true)
		{
			try {
				if(remainded < buffer.length)
				{
					out.write(buffer, 0, (int)remainded);
					break;
				}
				else 
				{
					out.write(buffer);
					remainded-=buffer.length;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
