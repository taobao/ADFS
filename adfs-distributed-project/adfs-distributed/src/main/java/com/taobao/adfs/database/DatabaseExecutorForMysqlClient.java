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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.impl.ResultSetImpl;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Index;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatabaseExecutorForMysqlClient extends DatabaseExecutor {
  static Map<FindOperator, String> operatorMap = new HashMap<FindOperator, String>();
  static {
    operatorMap.put(FindOperator.EQ, "=");
    operatorMap.put(FindOperator.LE, "<=");
    operatorMap.put(FindOperator.LT, "<");
    operatorMap.put(FindOperator.GE, ">=");
    operatorMap.put(FindOperator.GT, ">");
  }
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
    String port = MysqlServerController.getMysqlConf(conf, "mysqld.port", 40001);
    String hsServerAddress = host + ":" + port;
    String url = "jdbc:mysql://" + hsServerAddress + "/" + databaseName;
    String user = "root";
    String password = conf.get("database.executor.password", conf.get("mysql.server.password", "root"));
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

  public boolean open(DistributedDataRepositoryBaseOnTable repository, String dbname, String tableName, String[] columns)
      throws IOException {
    if (clients.isEmpty()) {
      createClient(dbname);
      Utilities.logInfo(repository.getLogger(), "create clients with number=" + clients.size());
    }
    return true;
  }

  public ResultSet findInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values,
      FindOperator operator, int limit, int offset) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("SELECT * FROM ").append(repository.tableName).append(" WHERE ");
      List<Index> indexList = repository.indexMap.get(indexName);
      List<String> indexListForColumnName = repository.indexMapForColumnName.get(indexName);
      for (int i = 0; i < indexList.size(); ++i) {
        if (indexList.get(i).index() > values.length - 1 || values[indexList.get(i).index()] == null) continue;
        sql.append(indexListForColumnName.get(i)).append(operatorMap.get(operator));
        if (values[indexList.get(i).index()] instanceof String) sql.append('\'').append(values[i]).append('\'');
        else sql.append(values[i]);
        if (i != values.length - 1) sql.append(" AND ");
      }
      sql.append(" LIMIT ").append(offset).append(',').append(limit).append(';');

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      ResultSet resultSet = statement.getResultSet();
      List<List<byte[]>> rowsForResultSet = new ArrayList<List<byte[]>>();
      while (resultSet.next()) {
        List<byte[]> columns = new ArrayList<byte[]>(repository.tableColumns.length);
        for (int i = 1; i <= repository.tableColumns.length; ++i) {
          columns.add(resultSet.getBytes(i));
        }
        rowsForResultSet.add(columns);
      }
      resultSet.close();
      statement.close();
      return new ResultSetImpl(rowsForResultSet, repository.tableColumns, "utf-8");
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values) + ", operator="
          + operator + ", sql=" + sql.toString(), t);
    }
  }

  public void insertInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values)
      throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("INSERT INTO ").append(repository.tableName).append(" VALUES(");
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

  public void updateInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      String[] values, FindOperator operator, int limit) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("UPDATE ").append(repository.tableName).append(" SET ");
      for (int i = 0; i < values.length; ++i) {
        sql.append(repository.tableColumns[i]).append("=");
        if (values[i] == null) sql.append("null");
        else if (!(values[i] instanceof String)) sql.append(values[i]);
        else sql.append('\'').append(values[i]).append('\'');
        if (i != values.length - 1) sql.append(',');
      }
      sql.append(' ').append(" WHERE ");
      List<Index> indexList = repository.indexMap.get(indexName);
      List<String> indexListForColumnName = repository.indexMapForColumnName.get(indexName);
      for (int i = 0; i < indexList.size(); ++i) {
        if (indexList.get(i).index() > keys.length - 1 || keys[indexList.get(i).index()] == null) continue;
        sql.append(indexListForColumnName.get(i)).append(operatorMap.get(operator));
        if (!(keys[indexList.get(i).index()] instanceof String)) sql.append(keys[indexList.get(i).index()]);
        else sql.append('\'').append(keys[indexList.get(i).index()]).append('\'');
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
          + Arrays.deepToString(values) + ", operator=" + operator + ", sql=" + sql.toString(), t);
    }
  }

  public void deleteInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      FindOperator operator, int limit) throws IOException {
    StringBuilder sql = new StringBuilder();
    try {
      sql.append("DELETE FROM ").append(repository.tableName).append(" WHERE ");
      List<Index> indexList = repository.indexMap.get(indexName);
      List<String> indexListForColumnName = repository.indexMapForColumnName.get(indexName);
      for (int i = 0; i < keys.length; ++i) {
        if (indexList.get(i).index() > keys.length - 1 || keys[indexList.get(i).index()] == null) continue;
        sql.append(indexListForColumnName.get(i)).append(operatorMap.get(operator));
        if (!(keys[indexList.get(i).index()] instanceof String)) sql.append(keys[indexList.get(i).index()]);
        else sql.append('\'').append(keys[indexList.get(i).index()]).append('\'');
        if (i != keys.length - 1) sql.append(" AND ");
      }
      sql.append(" LIMIT ").append(limit).append(';');

      int currentHsCientIndex = getClientIndex();
      Connection client = clients.get(currentHsCientIndex);
      Statement statement = client.createStatement();
      statement.execute(sql.toString());
      statement.close();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator
          + ", sql=" + sql.toString(), t);
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
