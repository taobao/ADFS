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

import java.io.PrintStream;

public class Output {
	public static  final int MAXLEN = 1000;
	private int fileNum;
	private int interval;
	private int[] times;
	private long startTime;
	private int timeIndex = 0;
	private PrintStream printer;
	
	public Output(int interval)
	{
		times = new int[MAXLEN];
		printer = System.out;
		this.interval = interval;
		this.fileNum = 0;
	}
	
	public Output(PrintStream printer, int interval)
	{
		this(interval);
		this.printer = printer;
	}
	
	public int getTimeIndex() {
		return timeIndex;
	}
	public void setTimeIndex(int timeIndex) {
		this.timeIndex = timeIndex;
	}
	
	public void setPrinter(PrintStream printer)
	{
		this.printer=printer;
	}
	public PrintStream getPrinter()
	{
		return this.printer;
	}
	
	public void printParamInfo(String parmInfo)
	{
		printer.println(parmInfo);
		printer.flush();
	}
	
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	
	public synchronized void flushTimeArray()
	{
		for(int i=0;i<timeIndex;i++)
		{
			printer.println(times[i]);
		}
		timeIndex = 0;
		printer.flush();
		printer.close();
	}
	
	public synchronized void recordTime(long currentTimeMillis) {
		fileNum++;
		if((fileNum%interval)!=0)
			return;
		if(timeIndex==times.length)
		{
			for(int i=0; i<times.length; i++)
			{
				printer.println(times[i]);
			}
			timeIndex = 0;
		}
		times[timeIndex++] = (int) (currentTimeMillis - startTime);
		startTime = currentTimeMillis;
	}
}
