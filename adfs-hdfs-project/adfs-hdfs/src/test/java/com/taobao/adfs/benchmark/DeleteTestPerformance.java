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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteTestPerformance {

	private static String deleteDir = "/create_write";
	private static FileSystem fs;
	public static int interval = 10;
	private static Output output;
	private static String logPath = "delete.log";
	public static void main(String[] args) throws IOException {
		fs = FileSystem.get(new Configuration());
		parseArgs(args);
		String parmInfo = "operation:delete;"+"threadNum:1"+";interval:"+interval
				+";deleteDir:"+deleteDir;
		output = new Output(interval);
		PrintStream printer = new PrintStream(new FileOutputStream(logPath),false);
		output.setPrinter(printer);
		output.printParamInfo(parmInfo);
		output.setStartTime(System.currentTimeMillis());
		doDelete();
		output.flushTimeArray();
	}

	private static void doDelete() throws IOException {
		FileStatus[] status = fs.listStatus(new Path(deleteDir));
		doDeleteInternal(status);
		fs.delete(new Path(deleteDir), false);
		output.recordTime(System.currentTimeMillis());
	}
	private static void doDeleteInternal(FileStatus[] status) throws IOException {
		Path file;
		if(status==null||status.length==0)
		{
			System.out.println(deleteDir + "does not exists, please run create_write first");
			return;
		}
		for(int i=0;i<status.length;i++)
		{
			file = status[i].getPath();
			if(!status[i].isDir())
			{
				fs.delete(file, false);
				output.recordTime(System.currentTimeMillis());
			}
			else{
				FileStatus[] dirStatus = fs.listStatus(file);
				doDeleteInternal(dirStatus);
				fs.delete(file, false);
				output.recordTime(System.currentTimeMillis());
			}
		}
	}

	private static void parseArgs(String[] args) {
		if (args == null || args.length == 0) {
			displayUsage();
			System.exit(1);
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-deleteDir")) {
				checkArgs(i + 1, args.length);
				deleteDir = args[++i];
			} else if (args[i].equals("-logPath")) {
				checkArgs(i + 1, args.length);
				logPath  = args[++i];
			}else if (args[i].equals("-interval")) {
				checkArgs(i + 1, args.length);
				interval = Integer.valueOf(args[++i]);
		    }else if (args[i].equals("-help")) {
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
			      "Usage: delete_test <options>\n" +
			      "Options:\n" +
			      "\t-deleteDir <the dir (including files of dir)to be delete. default is '/create_write'. This is not mandatory>\n" +
			      "\t-interval <interval of sampling. default is 100. " + "This is not mandatory>\n" +
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
