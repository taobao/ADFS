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

package com.taobao.adfs.performance.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class FsyncTest {
  static RandomAccessFile[] files = null;
  static FileOutputStream[] oStreams = null;
  static FileChannel[] fileChannels = null;
  static byte[] dataToWrite = null;
  static boolean onlyForce = false;

  public static void main(String[] args) throws InterruptedException, IOException {
    int threadNumber = (args != null && args.length > 0) ? Integer.valueOf(args[0]) : 1;
    int testNumberPerThread = (args != null && args.length > 1) ? Integer.valueOf(args[1]) : 1000;
    int writeBytes = (args != null && args.length > 2) ? Integer.valueOf(args[2]) : 1;
    if (args != null && args.length > 3 && "true".equals(args[3])) onlyForce = true;
    String filePath = "fsyncTest.txt";
    if (args != null && args.length > 4 && !args[4].trim().isEmpty()) filePath = args[4] + "/" + filePath;
    System.out.println("threadNumber       :" + threadNumber);
    System.out.println("testNumberPerThread:" + testNumberPerThread);
    System.out.println("writeBytes         :" + writeBytes);
    System.out.println("onlyForce          :" + onlyForce);
    System.out.println("fileToTest         :" + filePath);

    dataToWrite = new byte[writeBytes];
    for (int i = 0; i < writeBytes; ++i) {
      dataToWrite[i] = 90;
    }

    files = new RandomAccessFile[threadNumber];
    oStreams = new FileOutputStream[threadNumber];
    fileChannels = new FileChannel[threadNumber];
    for (int i = 0; i < threadNumber; ++i) {
      String path = filePath + "-" + i;
      new File(path).delete();
      files[i] = new RandomAccessFile(path, "rw");
      oStreams[i] = new FileOutputStream(files[i].getFD());
      fileChannels[i] = oStreams[i].getChannel();
      fileChannels[i].position(fileChannels[i].size());
      preallocate(i, testNumberPerThread * dataToWrite.length);
    }

    ThreadPoolExecutor applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber);
    MyRunnable[] myRunnables = new MyRunnable[threadNumber];
    for (int i = 0; i < threadNumber; ++i) {
      int idStart = i * testNumberPerThread;
      int idEnd = (i + 1) * testNumberPerThread;
      applyThreadPoolExecutor.execute(myRunnables[i] = new MyRunnable(i, idStart, idEnd));
    }
    long startTime = System.currentTimeMillis();
    long totalTimeForAllThread = 0;
    long lastShowTime = 0;
    for (int i = 0; i < threadNumber; ++i) {
      while (!myRunnables[i].done) {
        long timeSum = System.currentTimeMillis() - startTime;
        if (timeSum / 1000 > lastShowTime) {
          lastShowTime = timeSum / 1000;
          int currentTotalTime = 0;
          int currentTotalProcessNumber = 0;
          long currentTime = System.currentTimeMillis();
          for (int j = 0; j < threadNumber; ++j) {
            currentTotalTime += currentTime - myRunnables[i].startMilliTime;
            currentTotalProcessNumber += myRunnables[i].processNumber;
          }
          System.out.println("Progress=" + currentTotalProcessNumber + ", totalRT=" + currentTotalTime * 1.0
              / currentTotalProcessNumber + "ms");
        }
        Thread.sleep(10);
      }
      totalTimeForAllThread += myRunnables[i].totalNanoTime;
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println("totalTime=" + elapsedTime + "ms");
    System.out.println("totalOPS =" + threadNumber * testNumberPerThread * 1000.0 / elapsedTime);
    System.out.println("totalRT  =" + totalTimeForAllThread / 1000000.0 / threadNumber / testNumberPerThread + "ms");

    for (int i = 0; i < threadNumber; ++i) {
      oStreams[i].close();
      fileChannels[i].close();
      files[i].close();
    }

    System.exit(0);
  }

  static void testOp(int index, int idStart, int idEnd, MyRunnable myRunnable) throws IOException {
    for (int i = idStart; i < idEnd; ++i) {
      writeAndSync(index);
      myRunnable.processNumber++;
    }
  }

  // allocate a big chunk of data
  static void preallocate(int i, int addSize) throws IOException {
    ByteBuffer fill = ByteBuffer.allocateDirect(addSize); // preallocation
    fileChannels[i].write(fill, 0);
  }

  static void writeAndSync(int index) throws IOException {
    if (!onlyForce) oStreams[index].write(dataToWrite);
    fileChannels[index].force(false);
  }

  static class MyRunnable implements Runnable {
    int idStart = 0;
    int idEnd = 0;
    volatile boolean done = false;
    volatile long processNumber = 0;
    volatile long startMilliTime = 0;
    volatile long totalNanoTime = 0;
    int index = 0;

    MyRunnable(int i, int idStart, int idEnd) throws InterruptedException {
      this.index = i;
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run() {
      try {
        startMilliTime = System.currentTimeMillis();
        long startNanoTime = System.nanoTime();
        testOp(index, idStart, idEnd, this);
        totalNanoTime += System.nanoTime() - startNanoTime;
      } catch (Throwable t) {
        t.printStackTrace();
        // ignore this exception
      } finally {
        done = true;
      }
    }
  }
}
