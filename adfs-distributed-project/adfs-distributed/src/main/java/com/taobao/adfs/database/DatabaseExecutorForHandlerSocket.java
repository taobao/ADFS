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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.impl.HSClientImpl;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatabaseExecutorForHandlerSocket extends DatabaseExecutor {
  List<HSClient> readClients = null;
  List<HSClient> writeClients = null;
  Map<DistributedDataRepositoryBaseOnTable, Integer> repositories =
      new HashMap<DistributedDataRepositoryBaseOnTable, Integer>();

  public DatabaseExecutorForHandlerSocket(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
  }

  private void createClient(String databaseName) throws IOException {
    if (readClients == null) readClients = new ArrayList<HSClient>();
    else readClients.clear();
    if (writeClients == null) writeClients = new ArrayList<HSClient>();
    else writeClients.clear();
    int clientNumber = conf.getInt("database.executor.client.number", 1);
    new MysqlServerController().setMysqlDefaultConf(conf);
    String host = MysqlServerController.getMysqlConf(conf, "mysqld.bind-address", "localhost");
    host = host.replace("0.0.0.0", "localhost");
    int port = Integer.valueOf(MysqlServerController.getMysqlConf(conf, "mysqld.port", 40001));
    String readPort = MysqlServerController.getMysqlConf(conf, "mysqld.loose_handlersocket_port", port + 1);
    String writePort = MysqlServerController.getMysqlConf(conf, "mysqld.loose_handlersocket_port_wr", port + 2);
    int connectionPoolSize = conf.getInt("database.executor.connection.pool.size", 1);
    long timeout = conf.getLong("database.executor.timeout", 60000L);
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerHost=", host);
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerPort=", port);
    Utilities.logInfo(logger, "create DatabaseExecutor with readPort=", readPort);
    Utilities.logInfo(logger, "create DatabaseExecutor with writePort=", writePort);
    Utilities.logInfo(logger, "create DatabaseExecutor with connectionPoolSize=", connectionPoolSize);
    Utilities.logInfo(logger, "create DatabaseExecutor with timeout=", timeout);
    boolean independantRead = conf.getBoolean("database.executor.handlersocket.read.independent.enable", false);
    Utilities.logInfo(logger, "create DatabaseExecutor with readIndependentEnable=", independantRead);
    for (int i = 0; i < clientNumber; ++i) {
      HSClient hsClient = new HSClientImpl(host, Integer.valueOf(readPort), connectionPoolSize);
      hsClient.setOpTimeout(timeout);
      readClients.add(hsClient);
      hsClient = new HSClientImpl(host, Integer.valueOf(writePort), connectionPoolSize);
      hsClient.setOpTimeout(timeout);
      writeClients.add(hsClient);
    }
  }

  int getIndexOffset(DistributedDataRepositoryBaseOnTable repository, String indexName, int hsClientIndex,
      boolean isReadClient) {
    return (readClients.size() + writeClients.size())
        * (repositories.get(repository) + repository.tableIndexes.get(indexName))
        + (isReadClient ? hsClientIndex : readClients.size() + hsClientIndex);
  }

  public boolean open(DistributedDataRepositoryBaseOnTable repository, String dbname, String tableName, String[] columns)
      throws IOException {
    if (readClients == null || readClients.isEmpty() || writeClients == null || writeClients.isEmpty()) {
      createClient(dbname);
      Utilities.logInfo(repository.getLogger(), "create clients with number=" + writeClients.size());
    }

    int indexOffset = 0;
    for (DistributedDataRepositoryBaseOnTable existedRepository : repositories.keySet()) {
      indexOffset += existedRepository.tableIndexes.size();
    }
    repositories.put(repository, indexOffset);

    for (String indexName : repository.tableIndexes.keySet()) {
      try {
        for (int i = 0; i < readClients.size(); ++i) {
          int keyIndex = getIndexOffset(repository, indexName, i, true);
          if (!readClients.get(i).openIndex(keyIndex, dbname, tableName, indexName, columns))
            throw new IOException("fail to open index for handler socket read client");
          keyIndex = getIndexOffset(repository, indexName, i, false);
          if (!writeClients.get(i).openIndex(keyIndex, dbname, tableName, indexName, columns))
            throw new IOException("fail to open index for handler socket write client");
        }
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }

    return true;
  }

  public ResultSet findInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values,
      FindOperator operator, int limit, int offset) throws IOException {
    try {
      boolean independantRead = conf.getBoolean("database.executor.handlersocket.read.independent.enable", false);
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = (independantRead ? readClients : writeClients).get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(repository, indexName, currentHsCientIndex, independantRead);
      return hsClient.find(indexSerialNumber, values, operator, limit, offset);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values) + ", operator="
          + operator, t);
    }
  }

  public void insertInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values)
      throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = writeClients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(repository, indexName, currentHsCientIndex, false);
      boolean result = hsClient.insert(indexSerialNumber, values);
      if (!result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", values=" + Arrays.deepToString(values), t);
    }
  }

  public void updateInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys, String[] values,
      FindOperator operator, int limit) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = writeClients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(repository, indexName, currentHsCientIndex, false);
      int result = hsClient.update(indexSerialNumber, keys, values, operator, limit, 0);
      if (1 != result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", values="
          + Arrays.deepToString(values) + ", operator=" + operator, t);
    }
  }

  public void deleteInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      FindOperator operator, int limit) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = writeClients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(repository, indexName, currentHsCientIndex, false);
      int result = hsClient.delete(indexSerialNumber, keys, operator, limit, 0);
      if (1 != result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException(
          "indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator, t);
    }
  }

  public void close() throws IOException {
    if (writeClients != null) {
      for (HSClient hsClient : writeClients) {
        if (hsClient != null) hsClient.shutdown();
      }
      writeClients.clear();
      writeClients = null;
    }
    if (readClients != null) {
      for (HSClient hsClient : readClients) {
        if (hsClient != null) hsClient.shutdown();
      }
      readClients.clear();
      readClients = null;
    }
    if (repositories != null) {
      repositories.clear();
      repositories = null;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DatabaseExecutorForHandlerSocket=");
    if (writeClients == null || writeClients.isEmpty()) builder.append("no-read/write-client");
    else builder.append("{readHSClient[" + readClients.size() + "]=").append(readClients).append(
        ", writeHSClient[" + writeClients.size() + "]=").append(writeClients).append("}");
    return builder.toString();
  }

  @Override
  public int getClientNumber() {
    return writeClients.size();
  }
}
