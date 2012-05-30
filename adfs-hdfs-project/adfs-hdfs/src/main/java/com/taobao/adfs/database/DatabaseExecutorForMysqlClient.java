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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.code.hs4j.impl.ResultSetImpl;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Index;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.TableDescription;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatabaseExecutorForMysqlClient extends DatabaseExecutor {
  Configuration conf = null;
  List<Connection> clients = new ArrayList<Connection>();

  public DatabaseExecutorForMysqlClient(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
  }

  private void createClient(String databaseName) throws IOException {
    close();
    int clientNumber = conf.getInt("database.executor.client.number", 1);
    new MysqlServerController().setMysqlDefaultConf(conf);
    String host = MysqlServerController.getMysqlConf(conf, "mysqld.bind-address", "localhost");
    host = host.replace("0.0.0.0", "localhost");
    String port = MysqlServerController.getMysqlConf(conf, "mysqld.port", 50001);
    String hsServerAddress = host + ":" + port;
    String url = "jdbc:mysql://" + hsServerAddress + "/" + databaseName;
    String user = "root";
    String password = conf.get("database.executor.password", conf.get("mysql.server.password", "root"));
    Utilities.logInfo(logger, "create DatabaseExecutor with class=", getClass().getName());
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerHost=", host);
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerPort=", port);
    Utilities.logInfo(logger, "create DatabaseExecutor with jdbcUrl=", url);
    Utilities.logInfo(logger, "create DatabaseExecutor with username=", user);
    Utilities.logInfo(logger, "create DatabaseExecutor with password=", password);
    for (int i = 0; i < clientNumber; ++i) {
      try {
        Class.forName("com.mysql.jdbc.Driver");
        clients.add(DriverManager.getConnection(url, user, password));
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }
  }

  public boolean open(TableDescription tableDescripion) throws IOException {
    if (clients.isEmpty()) {
      createClient(tableDescripion.databaseName);
      Utilities.logInfo(logger, "create clients with number=" + clients.size());
    }
    return true;
  }

  public ResultSet findInternal(TableDescription tableDescripion, String indexName, String[] keys,
      Comparator comparator, int limit, int offset) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("SELECT * FROM ").append(tableDescripion.tableName).append(" WHERE ");
      List<Index> indexList = tableDescripion.indexMap.get(indexName);
      List<String> indexListForColumnName = tableDescripion.indexMapForColumnName.get(indexName);
      for (int i = 0; i < indexList.size(); ++i) {
        int indexOfIndex = indexList.get(i).index();
        if (indexOfIndex > keys.length - 1) continue;
        sql.append(indexListForColumnName.get(i));
        if (keys[indexOfIndex] == null) sql.append(" is NULL");
        else sql.append(comparator).append('\'').append(keys[indexOfIndex]).append('\'');
        if (i != keys.length - 1) sql.append(" AND ");
      }
      sql.append(" LIMIT ").append(offset).append(',').append(limit).append(';');

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      ResultSet resultSet = statement.getResultSet();
      List<List<byte[]>> rowsForResultSet = new ArrayList<List<byte[]>>();
      while (resultSet.next()) {
        List<byte[]> columns = new ArrayList<byte[]>(tableDescripion.tableColumns.length);
        for (int i = 1; i <= tableDescripion.tableColumns.length; ++i) {
          columns.add(resultSet.getBytes(i));
        }
        rowsForResultSet.add(columns);
      }
      resultSet.close();
      statement.close();
      return new ResultSetImpl(rowsForResultSet, tableDescripion.tableColumns, "utf-8");
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator="
          + comparator + ", sql=" + sql.toString(), t);
    }
  }

  public void insertInternal(TableDescription tableDescripion, String indexName, String[] values) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("INSERT INTO ").append(tableDescripion.tableName).append(" VALUES(");
      for (int i = 0; i < values.length; ++i) {
        if (values[i] == null) sql.append("null");
        else if (!(values[i] instanceof String)) sql.append(values[i]);
        else sql.append('\'').append(values[i]).append('\'');
        if (i != values.length - 1) sql.append(',');
      }
      sql.append(");");

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      statement.close();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", values=" + Arrays.deepToString(values) + ", sql="
          + sql.toString(), t);
    }
  }

  public void updateInternal(TableDescription tableDescripion, String indexName, String[] keys, String[] values,
      Comparator comparator, int limit) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("UPDATE ").append(tableDescripion.tableName).append(" SET ");
      for (int i = 0; i < values.length; ++i) {
        sql.append(tableDescripion.tableColumns[i]).append("=");
        if (values[i] == null) sql.append("null");
        else if (!(values[i] instanceof String)) sql.append(values[i]);
        else sql.append('\'').append(values[i]).append('\'');
        if (i != values.length - 1) sql.append(',');
      }
      sql.append(' ').append(" WHERE ");
      List<Index> indexList = tableDescripion.indexMap.get(indexName);
      List<String> indexListForColumnName = tableDescripion.indexMapForColumnName.get(indexName);
      for (int i = 0; i < indexList.size(); ++i) {
        int indexOfIndex = indexList.get(i).index();
        if (indexOfIndex > keys.length - 1) continue;
        sql.append(indexListForColumnName.get(i));
        if (keys[indexOfIndex] == null) sql.append(" is NULL");
        else sql.append(comparator).append('\'').append(keys[indexOfIndex]).append('\'');
        if (i != keys.length - 1) sql.append(" AND ");
      }
      sql.append(" LIMIT ").append(limit).append(';');

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      statement.close();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", values="
          + Arrays.deepToString(values) + ", comparator=" + comparator + ", sql=" + sql.toString(), t);
    }
  }

  public void deleteInternal(TableDescription tableDescripion, String indexName, String[] keys, Comparator comparator,
      int limit) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("DELETE FROM ").append(tableDescripion.tableName).append(" WHERE ");
      List<Index> indexList = tableDescripion.indexMap.get(indexName);
      List<String> indexListForColumnName = tableDescripion.indexMapForColumnName.get(indexName);
      for (int i = 0; i < keys.length; ++i) {
        int indexOfIndex = indexList.get(i).index();
        if (indexOfIndex > keys.length - 1) continue;
        sql.append(indexListForColumnName.get(i));
        if (keys[indexOfIndex] == null) sql.append(" is NULL");
        else sql.append(comparator).append('\'').append(keys[indexOfIndex]).append('\'');
        if (i != keys.length - 1) sql.append(" AND ");
      }
      sql.append(" LIMIT ").append(limit).append(';');

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      statement.close();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator="
          + comparator + ", sql=" + sql.toString(), t);
    }
  }

  public long countInternal(TableDescription tableDescripion) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("SELECT COUNT(*) FROM ").append(tableDescripion.tableName);
      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      ResultSet resultSet = statement.getResultSet();
      long count = resultSet.getLong(1);
      resultSet.close();
      statement.close();
      return count;
    } catch (Throwable t) {
      throw new IOException("sql=" + sql.toString(), t);
    }
  }

  public void close() throws IOException {
    for (Connection client : clients) {
      if (client != null) try {
        client.close();
      } catch (Throwable t) {
        // ignore this exception;
      }
    }
    clients.clear();
  }

  @Override
  public int getClientNumber() {
    return clients.size();
  }
}
