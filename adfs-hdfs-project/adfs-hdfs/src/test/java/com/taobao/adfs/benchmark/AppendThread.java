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

public class AppendThread implements Runnable {
	
	private int threadIndex;
	private FileSystem fs = null;
	private int fileNum = 0;
	private CountDownLatch fire;
	private CountDownLatch over;
	private Output output;
	private String appendPath="/create";
	public AppendThread(int threadIndex, FileSystem fs, int fileNum,String appendPath,
												CountDownLatch fire, CountDownLatch over, Output output)
	{
		this.threadIndex = threadIndex;
		this.fs = fs;
		this.fileNum = fileNum;
		this.fire = fire;
		this.over = over;
		this.output = output;
		this.appendPath = appendPath;
	}
	@Override
	public void run() {
		FSDataOutputStream out = null;
		Path createPath = new Path(appendPath);
		Path basePath = new Path(appendPath);
		try {
			if(!fs.exists(createPath ))
			{
				System.out.println("Do not exist files to be appended");
				System.out.println("please run creat_write first");
				System.exit(1);
			}
		} catch (IOException e1) {
			System.out.println("fs.exist " + createPath.toString() + " error!!!");
			e1.printStackTrace();
		}
		try {
			fire.await();
			for(int i = 0; i< fileNum; i++ )
			{
				out = fs.append(new Path(basePath, "file_" +(i+this.threadIndex*this.fileNum) ));
				output.recordTime(System.currentTimeMillis());
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
}
