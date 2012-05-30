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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class mysqlClientPermance2Test {
  static String[] args = null;

  static public void main(String[] args) throws SQLException, ClassNotFoundException, NumberFormatException,
      InterruptedException {
    mysqlClientPermance2Test.args = args;
    Class.forName("com.mysql.jdbc.Driver");
    String url = "jdbc:mysql://127.0.0.1:50011/nn_state";
    String user = "root";
    String password = "root";
    Connection client = DriverManager.getConnection(url, user, password);
    getResult(client.createStatement(), "delete FROM file WHERE id<" + Integer.MAX_VALUE + ";", 0);
    int threadNumber = (args != null && args.length > 1) ? Integer.valueOf(args[1]) : 1;
    MyRunnable[] myRunnables = new MyRunnable[threadNumber];
    ThreadPoolExecutor applyThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNumber);
    int testNumberPerThread = (args != null && args.length > 2) ? Integer.valueOf(args[2]) : 10000;
    for (int i = 0; i < threadNumber; ++i) {
      int idStart = i * testNumberPerThread;
      int idEnd = (i + 1) * testNumberPerThread;
      applyThreadPoolExecutor.execute(myRunnables[i] = new MyRunnable(client, idStart, idEnd));
    }
    for (int i = 0; i < threadNumber; ++i) {
      while (!myRunnables[i].done) {
        Thread.sleep(1000);
      }
    }
    client.close();
    System.exit(0);
  }

  static void testFind(Connection client, int idStart, int idEnd) throws NumberFormatException, SQLException,
      InterruptedException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client.createStatement(), "SELECT * FROM file WHERE id=0", i);
      getResult(client.createStatement(), "SELECT * FROM file WHERE parentId=0 and name='benchmarkerforstatemanager'",
          i);
      getResult(client.createStatement(),
          "SELECT * FROM file WHERE parentId=-1133800958 and name='20120304-122946-367'", i);
      getResult(client.createStatement(), "SELECT * FROM file WHERE parentId=-1133812794 and name='getfileinfo'", i);
      getResult(client.createStatement(), "SELECT * FROM file WHERE parentId=-1133812798 and name='folder-0000000000'",
          i);
      getResult(client.createStatement(),
          "SELECT * FROM file WHERE parentId=-1133812806 and name='srcFile-0000000000'", i);
    }
  }

  static void testInsert(Connection client, int idStart, int idEnd) throws NumberFormatException, SQLException,
      InterruptedException {
    for (int i = idStart; i < idEnd; ++i) {
      getResult(client.createStatement(), "SELECT * FROM file WHERE id=" + i + ";", i);
      getResult(client.createStatement(), "SELECT * FROM file WHERE parentId=0 and name='" + i + "';", i);
      getResult(client.createStatement(), "INSERT INTO file VALUES(" + i + "," + "0," + i + ",0,0,0,0,0,0," + i
          + ",null);", i);
    }
  }

  static void getResult(Statement statement, String sql, int i) throws SQLException, NumberFormatException,
      InterruptedException {
    long startTime = System.currentTimeMillis();
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
    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println("i=" + i + ", size=" + (rowsForResultSet == null ? 0 : rowsForResultSet.size())
        + ", elapsedTime=" + elapsedTime);
    if (args != null && args.length > 3 && args[3] != null && !args[3].isEmpty())
      Thread.sleep(Integer.valueOf(args[3]));
    statement.close();
  }

  static class MyRunnable implements Runnable {
    Connection client = null;
    int idStart = 0;
    int idEnd = 0;
    volatile boolean done = false;

    MyRunnable(Connection client, int idStart, int idEnd) {
      this.client = client;
      this.idStart = idStart;
      this.idEnd = idEnd;
    }

    @Override
    public void run() {
      try {
        if (args != null && args.length > 0 && "find".equals(args[0])) testFind(client, idStart, idEnd);
        if (args != null && args.length > 0 && "insert".equals(args[0])) testInsert(client, idStart, idEnd);
      } catch (Throwable t) {
        // ignore this exception
      } finally {
        done = true;
      }
    }
  }
}
