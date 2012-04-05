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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.command.AbstractCommand;
import com.taobao.adfs.iosimulator.command.BlockAllocatorCommand;
import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.TDHExecutor;

public class BlockAllocator {
  protected Logger logger = Logger.getLogger(getClass().getName());
  protected IExecutor executor;
  protected Random r = new Random();
  protected ExecutorService es;
  protected int numBlocks = -1;
  protected int numThreads = 10;
  protected AtomicLong counter = new AtomicLong(0);
  protected long[] idarray;
  protected int[] fidarray = null;

  // mapper of thread to block id list
  protected HashMap<Integer, List<Long>> mapper = 
    new HashMap<Integer, List<Long>>();
  
  protected void init() {
    try {
      executor = new TDHExecutor();
      es = Executors.newFixedThreadPool(numThreads);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
  
  protected void parseArgs(String[] args) {
    int iparm;
    for(String parm : args) {
      try {
        iparm = Integer.parseInt(parm);
        if(numBlocks==-1) {
          numBlocks=iparm;
          break;
        } 
      } catch (NumberFormatException e) {
        continue;
      }
    }
    System.out.println("Start Tests of " + this.getClass().getSimpleName());
    if(numBlocks == -1) {
      numBlocks = 100;
      System.out.println("Using default params: numBlocks=100");
    } else {
      System.out.println("Using params: numBlocks=" + numBlocks);
    }
  }
  
  protected void genRandomIds() {
    idarray = new long[numBlocks];
    long blkId;
    Set<Long> blkset = new HashSet<Long>();
    for(int i = 0; i < numBlocks; i++) {
      do {
        blkId = r.nextInt();
        if(!blkset.contains(blkId)) {
          blkset.add(blkId);
          break;
        }
      } while(true);
      idarray[i] = blkId;
    }
  }
  
  protected void runTest() {

    genRandomIds();
    int threadId;
    
    for(int i = 0; i < numBlocks; i++) {
      threadId = i%numThreads;
      if(!mapper.containsKey(threadId)) {
        List<Long> blklist = new ArrayList<Long>();
        blklist.add(idarray[i]);
        mapper.put(threadId, blklist);
      } else {
        List<Long> blklist = mapper.get(threadId);
        blklist.add(idarray[i]);
      }
    }

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch stop = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int tid = i;
      es.execute(new Runnable() {
        @Override
        public void run() {
          try {
            start.await();
            dotask(tid);
          } catch (Exception e) {
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
            while (!this.isInterrupted()) {
              System.out.print(counter + ".");
              TimeUnit.SECONDS.sleep(10);
            }
          } catch (InterruptedException e) {
          }
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
      float throughput = (float) 1.0 * numBlocks * 1000 / elapsed;
      float responseTime = (float) 1.0 * elapsed * numThreads / numBlocks;
      String ret = String.format("TotalBlockCreated:%d, Threads:%d, TotalTimeElapsed:%d(milsec),"
              + " Throughput:%.2f(persec), ResponseTime:%.2f(milsec)\n",
          numBlocks, numThreads, elapsed, throughput, responseTime);
      System.out.println("Test Result >>>> \n" + ret);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      es.shutdown();
      executor.close();
    }
  }
  
  private void dotask(int tid) {
    List<Long> list = mapper.get(tid);
    if(fidarray==null) {
      for(Long bid : list) {
        AbstractCommand cmd = 
          new BlockAllocatorCommand(executor, bid, -1, 
              bid, r.nextInt(), r.nextInt(10));
        cmd.execute();
        counter.incrementAndGet();
      }
    } else {
      int fidlen = fidarray.length;
      for(Long bid : list) {
        AbstractCommand cmd = 
          new BlockAllocatorCommand(executor, bid, -1, 
              bid, fidarray[r.nextInt(fidlen)], r.nextInt(10));
        cmd.execute();
        counter.incrementAndGet();
      }
    }
  }
  
  public long[] getBlockIdarray() {
    return idarray;
  }
  
  public void setParams(int numThreads, int numBlocks, int[] fids) {
    this.numThreads = numThreads;
    this.numBlocks = numBlocks;
    fidarray = fids;
  }
  
  public static void main(String[] args) {
    BlockAllocator ba = new BlockAllocator();
    ba.parseArgs(args);
    ba.init();
    ba.runTest();
  }
}
