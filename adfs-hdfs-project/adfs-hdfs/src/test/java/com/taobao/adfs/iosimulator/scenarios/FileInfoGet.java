/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 \*   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.iosimulator.scenarios;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.command.AbstractCommand;
import com.taobao.adfs.iosimulator.command.FileInfoGetCommand;
import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.TDHExecutor;

public class FileInfoGet {
  protected Logger logger = Logger.getLogger(getClass().getName());
  protected IExecutor executor;
  protected int threadNum = -1;
  protected int fileNum = -1; // toRead
  protected AtomicLong counter = new AtomicLong(0);
  protected Random r = new Random();
  protected int[] idarray;
  
  public static void main(String[] args) {
    FileInfoGet fig = new FileInfoGet();
    fig.parseArgs(args);
    fig.init();
    fig.runTest();
  }
  
  protected void parseArgs(String[] args) {
    int iparm;
    for(String parm : args) {
      try {
        iparm = Integer.parseInt(parm);
        if(threadNum==-1) {
          threadNum=iparm;
        } else {
          fileNum=iparm;
          break;
        }
      } catch (NumberFormatException e) {
        continue;
      }
    }
    
    if(fileNum <= 0 || threadNum <= 0) {
      System.out.println("Using default params: threadNum=1, fileNum=100");
      threadNum = 1;
      fileNum = 100;
    } else {
      System.out.println("Using params: threadNum=" + 
          threadNum + ", fileNum=" + fileNum);
    }
    
  }
  
  protected void init() {
    try {
      executor = new TDHExecutor();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
  
  protected void runTest() {
    if (fileNum < threadNum) {
      threadNum = fileNum;
    }
    
    ExecutorService es = Executors.newFixedThreadPool(threadNum);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch stop = new CountDownLatch(threadNum);
    
    for(int i = 0; i < threadNum; i++) {
      es.execute(new Runnable() {
        @Override
        public void run() {
          try {
            start.await();
            dotask();
          } catch(Exception e) {
           e.printStackTrace(); 
          } finally {
            stop.countDown();
          }
        }
      });
 
    }
    
    try {
      
      Thread t = new Thread() {
        public void run() {
          try {
            start.await();
            while(!this.isInterrupted()) {
              System.out.print(counter + ".");
              TimeUnit.SECONDS.sleep(10);
            }
          } catch (InterruptedException e) { }
          System.out.println(counter);
        }
      };
      t.start();
      System.out.println("Start Tests of " + this.getClass().getSimpleName());
      long startTime = System.currentTimeMillis();
      start.countDown();
      stop.await();
      t.interrupt();
      t.join();
      
      long elapsed = System.currentTimeMillis() - startTime;
      float throughput = (float)1.0*fileNum*1000/elapsed;
      float responseTime = (float)1.0*elapsed*threadNum/fileNum;
      String ret = String.format("TotalFileReaded:%d, Threads:%d, TotalTimeElapsed:%d(milsec)," +
          " Throughput:%.2f(persec), ResponseTime:%.2f(milsec)\n", 
          fileNum, threadNum, elapsed, throughput, responseTime);
      System.out.println("Test Result >>>> \n" + ret);
      
    } catch(Exception e) {
      e.printStackTrace();
    } finally {
      es.shutdown();
      executor.close();
    }
  }
  
  private void dotask() {
    while(true) {
      AbstractCommand cmd = new FileInfoGetCommand(executor, r.nextInt());
      cmd.execute();
      if(counter.incrementAndGet() >= fileNum) {
        break;
      }
    }
  }
  
  public int[] getCreatedFileIds() {
    return idarray;
  }
  
  public void setParams(int threadNum, int fileNum) {
    this.threadNum = threadNum;
    this.fileNum = fileNum;
  }
}
