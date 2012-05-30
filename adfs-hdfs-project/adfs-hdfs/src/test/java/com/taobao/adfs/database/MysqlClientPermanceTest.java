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

package com.taobao.adfs.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class MysqlClientPermanceTest {
  static String[] args = null;

  static Connection getClient(String url, String username, String password) throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }

  static String opType = null;

  static public void main(String[] args) throws SQLException, ClassNotFoundException, NumberFormatException,
      InterruptedException {
    MysqlClientPermanceTest.args = args;
    Class.forName("com.mysql.jdbc.Driver");
    String url = "jdbc:mysql://127.0.0.1:50001/nn_state";
    if (args != null && args.length > 4) url = "jdbc:mysql://" + args[4] + "/nn_state";
    String username = "root";
    String password = "root";
    opType = args[0];
    if (args != null && args.length > 0 && args[0].startsWith("insert")) {
      Connection client = getClient(url, username, password);
      Statement statement = client.createStatement();
      statement.execute("truncate table file;");
      statement.execute("truncate table block;");
      statement.execute("truncate table datanode;");
      statement.close();
      client.close();
    }
    int threadNumber = (args != null && args.length > 1) ? Integer.valueOf(args[1]) : 1;
    MyRunnable[] myRunnables = new MyRunnable[threadNumber];
    ThreadPoolExecutor applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber);
    int testNumberPerThread = (args != null && args.length > 2) ? Integer.valueOf(args[2]) : 10000;
    for (int i = 0; i < threadNumber; ++i) {
      int idStart = i * testNumberPerThread;
      int idEnd = (i + 1) * testNumberPerThread;
      applyThreadPoolExecutor.execute(myRunnables[i] = new MyRunnable(url, username, password, idStart, idEnd));
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

    System.exit(0);
  }

  static void test(Connection client, int idStart, int idEnd, MyRunnable myRunnable) throws NumberFormatException,
      SQLException, InterruptedException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client.createStatement(), i);
      myRunnable.processNumber++;
    }
  }

  static void getResult(Statement statement, int i) throws SQLException, NumberFormatException, InterruptedException {
    long startTime = System.currentTimeMillis();

    String sql = null;
    if (opType.equals("findFile")) {
      sql = "SELECT * FROM file WHERE id=" + i;
      statement.execute(sql);
      ResultSet resultSet = statement.getResultSet();
      List<List<byte[]>> rowsForResultSet = null;
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
      sql = "SELECT * FROM block WHERE id=" + i + " and datanodeId=" + i;
      statement.execute(sql);
      ResultSet resultSet = statement.getResultSet();
      List<List<byte[]>> rowsForResultSet = null;
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
      sql = "SELECT * FROM file WHERE id=" + i;
      statement.execute(sql);
      ResultSet resultSet = statement.getResultSet();
      List<List<byte[]>> rowsForResultSet = null;
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
      sql = "SELECT * FROM block WHERE id=" + i + " and datanodeId=" + i;
      statement.execute(sql);
      resultSet = statement.getResultSet();
      rowsForResultSet = null;
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
      sql = "INSERT INTO file VALUES(" + i + ",0," + i + ",0,0,0,0,0,0," + i + ",null);";
      statement.execute(sql);
    } else if (opType.equals("insertBlock")) {
      sql = "INSERT INTO block VALUES(" + i + "," + i + ",0,0,0,0," + i + ",null);";
      statement.execute(sql);
    } else if (opType.equals("insert")) {
      sql = "INSERT INTO file VALUES(" + i + ",0," + i + ",0,0,0,0,0,0," + i + ",null)";
      statement.execute(sql);
      sql = "INSERT INTO block VALUES(" + i + "," + i + ",0,0,0,0," + i + ",null);";
      statement.execute(sql);
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    if (args != null && args.length > 3 && "true".equals(args[3])) {
      System.out.println("i=" + i + ", elapsedTime=" + elapsedTime + "ms");
    }
    statement.close();
  }

  static class MyRunnable implements Runnable {
    Connection client = null;
    int idStart = 0;
    int idEnd = 0;
    volatile boolean done = false;
    volatile long processNumber = 0;
    volatile long startMilliTime = 0;
    volatile long totalNanoTime = 0;

    MyRunnable(String url, String username, String password, int idStart, int idEnd) throws SQLException {
      this.client = getClient(url, username, password);
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run() {
      try {
        startMilliTime = System.currentTimeMillis();
        long startNanoTime = System.nanoTime();
        test(client, idStart, idEnd, this);
        totalNanoTime += System.nanoTime() - startNanoTime;
        client.close();
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        done = true;
      }
    }
  }
}
