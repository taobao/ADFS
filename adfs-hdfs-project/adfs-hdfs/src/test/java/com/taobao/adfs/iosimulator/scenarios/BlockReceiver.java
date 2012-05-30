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

package com.taobao.adfs.iosimulator.scenarios;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.command.AbstractCommand;
import com.taobao.adfs.iosimulator.command.BlockReceivedCommand;
import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.TDHExecutor;

public class BlockReceiver {

  protected Logger logger = Logger.getLogger(getClass().getName());
  protected IExecutor executor;
  protected int dataNum = 100;
  protected int threadNum = -1;
  protected int fileNum = -1;
  protected int blockNum = -1;
  protected AtomicLong counter = new AtomicLong(0);
  protected Random r = new Random();
  protected int[] fidarray;
  protected long[] blkarray;
  // mapper of thread to file id list
  protected HashMap<Integer, List<Long>> mapper = 
    new HashMap<Integer, List<Long>>();
  
  protected void init() {
    try {
      executor = new TDHExecutor();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
  
  protected void prepare() {
    
    DatanodeReg dr = new DatanodeReg();
    dr.setDatanodeNum(dataNum);
    dr.init();
    dr.runTest();
    
    FileCreator fc = new FileCreator();
    fc.setParams(threadNum, fileNum);
    fc.init();
    fc.runTest();
    fidarray = fc.getCreatedFileIds();
    
    BlockAllocator ba = new BlockAllocator();
    ba.setParams(threadNum, blockNum, fidarray);
    ba.init();
    ba.runTest();
    blkarray = ba.getBlockIdarray();
  }
  
  protected void parseArgs(String[] args) {
    int iparm;
    for(String parm : args) {
      try {
        iparm = Integer.parseInt(parm);
        if(threadNum==-1) {
          threadNum=iparm;
        } else if(fileNum==-1){
          fileNum=iparm;
        } else if(blockNum==-1) {
          blockNum=iparm;
        }
      } catch (NumberFormatException e) {
        continue;
      }
    }
    
    if(fileNum <= 0 || threadNum <= 0 || blockNum<= 0) {
      System.out.println("Using default params: threadNum=10, fileNum=100, blockNum=10000");
      threadNum = 10;
      fileNum = 100;
      blockNum=10000;
    } else {
      System.out.println("Using params: threadNum=" + 
          threadNum + ", fileNum=" + fileNum + ", blockNum=" + blockNum);
    }
    
  }
  
  protected void runTest() {
    
    int threadId;
    int end = blkarray.length;
    for(int i = 0; i < end; i++) {
      threadId = i%threadNum;
      if(!mapper.containsKey(threadId)) {
        List<Long> blklist = new ArrayList<Long>();
        blklist.add(blkarray[i]);
        mapper.put(threadId, blklist);
      } else {
        List<Long> blklist = mapper.get(threadId);
        blklist.add(blkarray[i]);
      }
    }
    
    ExecutorService es = Executors.newFixedThreadPool(threadNum);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch stop = new CountDownLatch(threadNum);
    
    for(int i = 0; i < threadNum; i++) {
      final int tid = i;
      es.execute(new Runnable() {
        @Override
        public void run() {
          try {
            start.await();
            dotask(tid);
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
      float throughput = (float)1.0*blockNum*1000/elapsed;
      float responseTime = (float)1.0*elapsed*threadNum/blockNum;
      String ret = String.format("TotalBlockReceived:%d, Threads:%d, TotalTimeElapsed:%d(milsec)," +
          " Throughput:%.2f(persec), ResponseTime:%.2f(milsec)\n", 
          blockNum, threadNum, elapsed, throughput, responseTime);
      System.out.println("Test Result >>>> \n" + ret);
      
    } catch(Exception e) {
      e.printStackTrace();
    } finally {
      es.shutdown();
      executor.close();
    }
  }
  
  protected void dotask(int tid) {
    List<Long> list = mapper.get(tid);
    for(Long blkId : list) {
      AbstractCommand cmd = new BlockReceivedCommand(executor, 
          blkId, r.nextInt(dataNum));
      cmd.execute();
      counter.incrementAndGet();
    }
  }
  
  public static void main(String[] args) {
    BlockReceiver br = new BlockReceiver();
    br.parseArgs(args);
    br.prepare();
    br.init();
    br.runTest();
  }
}
