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

public class RenameTestPerformance {

	private static String renameDir = "/create_write";
	private static FileSystem fs;
	public static int interval = 10;
	private static Output output;
	private static String logPath = "rename.log";
	public static void main(String[] args) throws IOException {
		fs = FileSystem.get(new Configuration());
		parseArgs(args);
		output = new Output(interval);
		PrintStream printer = new PrintStream(new FileOutputStream(logPath),false);
		output.setPrinter(printer);
		String parmInfo = "operation:rename;"+"threadNum:1"+";interval:"+interval
				+";renameDir:"+renameDir;
		output.printParamInfo(parmInfo);
		output.setStartTime(System.currentTimeMillis());
		doRename();
		output.flushTimeArray();
		System.out.println("run successfully");
	}

	private static void doRename() throws IOException {
		FileStatus[] status = fs.listStatus(new Path(renameDir));
		doRenameInternal(status);
	}
	private static void doRenameInternal(FileStatus[] status) throws IOException {
		Path file;
		if(status==null||status.length==0)
		{
			System.out.println(renameDir + "does not exist, please run create_write first");
			return;
		}
		for(int i=0;i<status.length;i++)
		{
			file = status[i].getPath();
			if(!status[i].isDir())
			{
				fs.rename(file, new Path(file.toString()+"_rename"));
				output.recordTime(System.currentTimeMillis());
			}
			else{
				FileStatus[] dirStatus = fs.listStatus(file);
				doRenameInternal(dirStatus);
				fs.rename(file, new Path(file.toString()+"_rename"));
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
			if (args[i].equals("-renameDir")) {
				checkArgs(i + 1, args.length);
				renameDir = args[++i];
			} else if (args[i].equals("-logPath")) {
				checkArgs(i + 1, args.length);
				logPath  = args[++i];
			}  else if (args[i].equals("-help")) {
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
			      "Usage: rename_test <options>\n" +
			      "Options:\n" +
			      "\t-renameDir <the dir (including files of dir)to be rename. default is '/create_write'. This is not mandatory>\n" +
			      "\t-interval <interval of sampling. default is 100. " + "This is not mandatory>\n" +
			      "\t-logPath <the full path of log file. default is ./rename.log. " + "This is not mandatory>\n" +
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
