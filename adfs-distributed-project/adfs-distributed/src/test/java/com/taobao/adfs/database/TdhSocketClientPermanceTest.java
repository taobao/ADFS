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

package com.taobao.adfs.database;

import com.taobao.adfs.database.tdhsocket.client.TDHSClient;
import com.taobao.adfs.database.tdhsocket.client.TDHSClientImpl;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

public class TdhSocketClientPermanceTest {
  static String[] args = null;

  static void clear() throws SQLException, ClassNotFoundException {
    if (args != null && args.length > 0 && args[0].startsWith("insert")) {
      Class.forName("com.mysql.jdbc.Driver");
      String address = "127.0.0.1:40001";
      if (args != null && args.length > 4) address = args[4];
      String url = "jdbc:mysql://" + address.substring(0, address.length() - 1) + "1/nn_state";
      Connection client = DriverManager.getConnection(url, "root", "root");
      Statement statement = client.createStatement();
      statement.execute("truncate table file;");
      statement.execute("truncate table block;");
      statement.execute("truncate table datanode;");
      statement.close();
      client.close();
    }
  }

  static String opType = null;

  // static HSClient client = null;

  // 0 insert/find 1 thread num 2 count/thread 3 rt显示 4 host:port 5 连接数
  static public void main(String[] args) throws SQLException, ClassNotFoundException, NumberFormatException,
      InterruptedException, IOException, TimeoutException, TDHSException {
    TdhSocketClientPermanceTest.args = args;
    clear();
    opType = args[0];
    int threadNumber = (args != null && args.length > 1) ? Integer.valueOf(args[1]) : 1;
    MyRunnable[] myRunnables = new MyRunnable[threadNumber];
    ThreadPoolExecutor applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber);
    int testNumberPerThread = (args != null && args.length > 2) ? Integer.valueOf(args[2]) : 10000;
    TDHSClient client = getClient();
    for (int i = 0; i < threadNumber; ++i) {
      int idStart = i * testNumberPerThread;
      int idEnd = (i + 1) * testNumberPerThread;
      applyThreadPoolExecutor.execute(myRunnables[i] = new MyRunnable(client, i, idStart, idEnd));
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
    if (args[0].equals("find") || args[0].equals("insert")) {
      testNumberPerThread *= 2;
      System.out.println("totalTime=" + elapsedTime + "ms");
      System.out.println("totalOPS =" + threadNumber * testNumberPerThread * 1000.0 / elapsedTime);
      System.out.println("totalRT  =" + totalTimeForAllThread / 1000000.0 / threadNumber / testNumberPerThread + "ms");
    } else {
      System.out.println("totalTime=" + elapsedTime + "ms");
      System.out.println("totalOPS =" + threadNumber * testNumberPerThread * 1000.0 / elapsedTime);
      System.out.println("totalRT  =" + totalTimeForAllThread / 1000000.0 / threadNumber / testNumberPerThread + "ms");
    }

    client.shutdown();
    System.exit(0);
  }

  static TDHSClient getClient() throws SQLException, IOException, InterruptedException, TimeoutException, TDHSException {
    // if (client != null) return client;
    String address = "127.0.0.1:40004";
    if (args != null && args.length > 4) address = args[4];
    int poolSize = 1;
    if (args != null && args.length > 5) poolSize = Integer.valueOf(args[5]);
    String host = address.split(":")[0];
    int port = Integer.valueOf(address.split(":")[1]);
    return new TDHSClientImpl(new InetSocketAddress(host, port), poolSize, 6000000);
  }

  static void testOp(TDHSClient client, int indexId, int idStart, int idEnd, MyRunnable myRunnable)
      throws NumberFormatException, SQLException, InterruptedException, TimeoutException, TDHSException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client, indexId, i);
      myRunnable.processNumber++;
    }
  }

  static String[] fileCols =
      new String[] { "id", "parentId", "name", "length", "blockSize", "replication", "atime", "mtime", "owner", "version", "operateIdentifier" };
  static String[] blockCols =
      new String[] { "id", "datanodeId", "numbytes", "generationStamp", "fileId", "fileIndex", "version", "operateIdentifier" };

  static void getResult(TDHSClient client, int indexId, int i) throws SQLException, NumberFormatException,
      InterruptedException, TimeoutException, TDHSException {
    long startTime = System.currentTimeMillis();
    List<List<byte[]>> rowsForResultSet = null;

    // hsClient
    // .openIndex(
    // i,
    // "nn_state",
    // "file",
    // "PRIMARY",
    // new String[]{"id", "parentId", "name", "length", "blockSize", "replication", "atime", "mtime",
    // "owner", "version", "operateIdentifier"});
    if (opType.equals("findFile")) {
      ResultSet resultSet = null;
      TDHSResponse response =
          client.get("nn_state", "file", "PRIMARY", fileCols, new String[][] { new String[] { String.valueOf(i) } });
      if (response != null) {
        resultSet = response.getResultSet();
      }
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
    } else if (opType.equals("findBlock")) {
      ResultSet resultSet = null;
      TDHSResponse response =
          client.get("nn_state", "block", "PRIMARY", blockCols,
              new String[][] { new String[] { String.valueOf(i), String.valueOf(i) } });
      if (response != null) {
        resultSet = response.getResultSet();
      }
      if (resultSet != null) {
        rowsForResultSet = new ArrayList<List<byte[]>>();
        while (resultSet.next()) {
          List<byte[]> columns = new ArrayList<byte[]>(11);
          for (int j = 1; j <= 8; ++j) {
            columns.add(resultSet.getBytes(j));
          }
          rowsForResultSet.add(columns);
        }
        resultSet.close();
      }
    } else if (opType.equals("find")) {
      ResultSet resultSet = null;
      TDHSResponse response =
          client.get("nn_state", "file", "PRIMARY", fileCols, new String[][] { new String[] { String.valueOf(i) } });
      if (response != null) {
        resultSet = response.getResultSet();
      }
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

      response =
          client.get("nn_state", "block", "PRIMARY", blockCols,
              new String[][] { new String[] { String.valueOf(i), String.valueOf(i) } });
      if (response != null) {
        resultSet = response.getResultSet();
      }
      if (resultSet != null) {
        rowsForResultSet = new ArrayList<List<byte[]>>();
        while (resultSet.next()) {
          List<byte[]> columns = new ArrayList<byte[]>(11);
          for (int j = 1; j <= 8; ++j) {
            columns.add(resultSet.getBytes(j));
          }
          rowsForResultSet.add(columns);
        }
        resultSet.close();
      }
    } else if (opType.equals("insertFile")) {
      client.createStatement(indexId+1)
          .insert(
              "nn_state",
              "file",
              fileCols,
              new String[] { String.valueOf(i), "0", String.valueOf(i), "0", "0", "0", "0", "0", "0", String.valueOf(i), "null" });
    } else if (opType.equals("insertBlock")) {
      client.createStatement(indexId+1).insert("nn_state", "block", blockCols,
          new String[] { String.valueOf(i), String.valueOf(i), "0", "0", "0", "0", String.valueOf(i), "null" });
    } else if (opType.equals("insert")) {
      client.createStatement(indexId+1)
          .insert(
              "nn_state",
              "file",
              fileCols,
              new String[] { String.valueOf(i), "0", String.valueOf(i), "0", "0", "0", "0", "0", "0", String.valueOf(i), "null" });
      client.createStatement(indexId+1).insert("nn_state", "block", blockCols,
          new String[] { String.valueOf(i), String.valueOf(i), "0", "0", "0", "0", String.valueOf(i), "null" });
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (args != null && args.length > 3 && "true".equals(args[3])) {
      System.out.println("i=" + i + ", size=" + (rowsForResultSet == null ? 0 : rowsForResultSet.size())
          + ", elapsedTime=" + elapsedTime + "ms");
    }
  }

  static class MyRunnable implements Runnable {
    TDHSClient client = null;
    int idStart = 0;
    int idEnd = 0;
    volatile boolean done = false;
    volatile long processNumber = 0;
    volatile long startMilliTime = 0;
    volatile long totalNanoTime = 0;
    int indexId = 0;

    MyRunnable(TDHSClient client, int i, int idStart, int idEnd) throws SQLException, IOException,
        InterruptedException, TimeoutException, TDHSException {
      this.client = client;
      this.indexId = i;
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run() {
      try {
        startMilliTime = System.currentTimeMillis();
        long startNanoTime = System.nanoTime();
        testOp(client, indexId, idStart, idEnd, this);
        totalNanoTime += System.nanoTime() - startNanoTime;
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
