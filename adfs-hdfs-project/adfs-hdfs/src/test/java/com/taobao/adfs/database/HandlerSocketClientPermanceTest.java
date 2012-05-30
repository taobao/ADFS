/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.exception.HandlerSocketException;
import com.google.code.hs4j.impl.HSClientImpl;
import com.google.code.hs4j.network.core.impl.AbstractController;
import com.google.code.hs4j.network.nio.impl.SelectorManager;

public class HandlerSocketClientPermanceTest {
  static String[] args = null;

  static void clear() throws SQLException, ClassNotFoundException {
    if (args != null && args.length > 0 && "insert".equals(args[0])) {
      Class.forName("com.mysql.jdbc.Driver");
      String address = "127.0.0.1:50001";
      if (args != null && args.length > 4) address = args[4];
      String url = "jdbc:mysql://" + address.split(":")[0] + ":50001/nn_state";
      Connection client = DriverManager.getConnection(url, "root", "root");
      Statement statement = client.createStatement();
      String sql = "truncate table file;";
      statement.execute(sql);
      statement.close();
      client.close();
    }
  }

  // static HSClient client = null;

  static HSClient getClient(int i) throws SQLException, IOException, InterruptedException, TimeoutException,
      HandlerSocketException {
    LogManager.getLogger(AbstractController.class).setLevel(Level.ERROR);
    LogManager.getLogger(SelectorManager.class).setLevel(Level.ERROR);
    // if (client != null) return client;
    String address = "127.0.0.1:50003";
    if (args != null && args.length > 4) address = args[4];
    int poolSize = 1;
    if (args != null && args.length > 5) poolSize = Integer.valueOf(args[5]);
    String host = address.split(":")[0];
    int port = Integer.valueOf(address.split(":")[1]);
    HSClient hsClient = new HSClientImpl(host, port, poolSize);
    hsClient.setOpTimeout(6000000L);
    hsClient
        .openIndex(
            i,
            "nn_state",
            "file",
            "PRIMARY",
            new String[] { "id", "parentId", "name", "length", "blockSize", "replication", "atime", "mtime", "owner", "version", "operateIdentifier" });
    // client = hsClient;
    return hsClient;
  }

  static public void main(String[] args) throws SQLException, ClassNotFoundException, NumberFormatException,
      InterruptedException, IOException, TimeoutException, HandlerSocketException {
    HandlerSocketClientPermanceTest.args = args;
    clear();
    int threadNumber = (args != null && args.length > 1) ? Integer.valueOf(args[1]) : 1;
    MyRunnable[] myRunnables = new MyRunnable[threadNumber];
    ThreadPoolExecutor applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber);
    int testNumberPerThread = (args != null && args.length > 2) ? Integer.valueOf(args[2]) : 10000;
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

    // client.shutdown();
    System.exit(0);
  }

  static void testFind(HSClient client, int indexId, int idStart, int idEnd, MyRunnable myRunnable)
      throws NumberFormatException, SQLException, InterruptedException, TimeoutException, HandlerSocketException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client, indexId, i, true);
      myRunnable.processNumber++;
    }
  }

  static void testInsert(HSClient client, int indexId, int idStart, int idEnd, MyRunnable myRunnable)
      throws NumberFormatException, SQLException, InterruptedException, TimeoutException, HandlerSocketException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client, indexId, i, false);
      myRunnable.processNumber++;
    }
  }

  static void getResult(HSClient client, int indexId, int i, boolean find) throws SQLException, NumberFormatException,
      InterruptedException, TimeoutException, HandlerSocketException {
    long startTime = System.currentTimeMillis();
    List<List<byte[]>> rowsForResultSet = null;
    if (find) {
      ResultSet resultSet = client.find(indexId, new String[] { String.valueOf(i) });
      if (resultSet != null) {
        rowsForResultSet = new ArrayList<List<byte[]>>();
        while (resultSet.next()) {
          List<byte[]> columns = new ArrayList<byte[]>(11);
          for (int j = 1; j <= 11; ++j) {
            columns.add(resultSet.getBytes(j));
          }
          rowsForResultSet.add(columns);
        }
        resultSet.close();
      }
    } else client
        .insert(indexId, new String[] { String.valueOf(i), "0", String.valueOf(i), "0", "0", "0", "0", "0", "0", String
            .valueOf(i), "null" });
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (args != null && args.length > 3 && "true".equals(args[3])) {
      System.out.println("i=" + i + ", size=" + (rowsForResultSet == null ? 0 : rowsForResultSet.size())
          + ", elapsedTime=" + elapsedTime + "ms");
    }
  }

  static class MyRunnable implements Runnable {
    HSClient client = null;
    int idStart = 0;
    int idEnd = 0;
    volatile boolean done = false;
    volatile long processNumber = 0;
    volatile long startMilliTime = 0;
    volatile long totalNanoTime = 0;
    int indexId = 0;

    MyRunnable(int i, int idStart, int idEnd) throws SQLException, IOException, InterruptedException, TimeoutException,
        HandlerSocketException {
      this.client = getClient(i);
      this.indexId = i;
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run() {
      try {
        startMilliTime = System.currentTimeMillis();
        long startNanoTime = System.nanoTime();
        if (args != null && args.length > 0 && "find".equals(args[0])) testFind(client, indexId, idStart, idEnd, this);
        if (args != null && args.length > 0 && "insert".equals(args[0]))
          testInsert(client, indexId, idStart, idEnd, this);
        totalNanoTime += System.nanoTime() - startNanoTime;
        client.shutdown();
      } catch (Throwable t) {
        t.printStackTrace();
        System.out.println(indexId);
        // ignore this exception
      } finally {
        done = true;
      }
    }
  }
}
