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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class CreateWrite {

	private static int  threadNum = 1;
	private static long  fileSize = 0;
	private static long  blockSize = 512;
	private static int  fileNumPerThread = 1;
	private static int  interval = 10;
	private static String logPath = "create_write.log";
	public static void main(String[] args) throws IOException {
		parseArgs(args);
		runTest();
	}

	private static void runTest() throws IOException {
		CountDownLatch fire = new CountDownLatch(1);
		CountDownLatch over = new CountDownLatch(threadNum);
		Thread[] threads = new Thread[threadNum];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Output output = new Output(interval);
		PrintStream printer = new PrintStream(new FileOutputStream(logPath,false));
		output.setPrinter(printer);
		String parmInfo = "operation:create_write;"+"threadNum:"+threadNum+";blockSize:"+blockSize+
				";fileSize:"+fileSize+";fileNumPerThread:"+fileNumPerThread+";interval:"+interval;
		output.printParamInfo(parmInfo);
		for(int i =0; i<threadNum; i++)
		{
			threads[i] = new Thread(new WriterThread(i, fs, fileSize, blockSize, 
												fileNumPerThread, fire, over, output));
			threads[i].start();
		}
		try {
			fire.countDown();
			output.setStartTime(System.currentTimeMillis());
			over.await();
			output.flushTimeArray();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void parseArgs(String[] args) {
		if (args == null || args.length == 0) {
			displayUsage();
		    System.exit(-1);
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-fileNumPerThread")) {
				fileNumPerThread = Integer.valueOf(args[++i]);
			} else if (args[i].equals("-threadNum")) {
				checkArgs(i + 1, args.length);
				threadNum = Integer.valueOf(args[++i]);
			} else if (args[i].equals("-logPath")) {
				checkArgs(i + 1, args.length);
				logPath = args[++i];
			}  else if (args[i].equals("-blockSize")) {
				checkArgs(i + 1, args.length);
				blockSize = Long.parseLong(args[++i]);
			} else if (args[i].equals("-fileSize")) {
				checkArgs(i + 1, args.length);
				fileSize = Long.parseLong(args[++i]);
			} else if (args[i].equals("-interval")) {
				checkArgs(i + 1, args.length);
				interval = Integer.valueOf(args[++i]);
			} else if (args[i].equals("-help")) {
				displayUsage();
				System.exit(-1);
		    }else{
		    	displayUsage();
				System.exit(-1);
		    }
		}
	}

	private static void displayUsage() {
		String usage =
			      "Usage: create_write <options>\n" +
			      "Options:\n" +
			      "\t-fileNumPerThread <number of file to create by one thread. default is 1. This is not mandatory>\n" +
			      "\t-threadNum <number of thread. default is 1. This is not mandatory>\n" +
			      "\t-blockSize <Block size in bytes. default is 512. " +  "This is not mandatory>\n" +
			      "\t-fileSize <size of file. default is 0. " + "This is not mandatory>\n" +
			      "\t-interval <interval of sampling. default is 100. " + "This is not mandatory>\n" +
			      "\t-logPath <the full path of log file. default is ./create_write.log. " + "This is not mandatory>\n" +
			      "\t-help: Display the help statement\n";
			    
		System.out.println(usage);
	}
	public static void checkArgs(final int index, final int length) {
	    if (index == length) {
	      displayUsage();
	      System.exit(-1);
	    }
	  }
}
