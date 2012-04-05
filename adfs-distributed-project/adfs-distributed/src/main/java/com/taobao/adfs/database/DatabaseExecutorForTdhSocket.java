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

import com.google.code.hs4j.FindOperator;
import com.taobao.adfs.database.tdhsocket.client.TDHSClient;
import com.taobao.adfs.database.tdhsocket.client.TDHSClientImpl;
import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.request.ValueEntry;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponseEnum;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.util.Utilities;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-3-5 下午3:31
 */
public class DatabaseExecutorForTdhSocket extends DatabaseExecutor {
  private Configuration conf = null;

  private List<TDHSClient> clients = null;

  public DatabaseExecutorForTdhSocket(Configuration conf) {
    this.conf = (conf == null) ? new Configuration(false) : conf;
  }

  private void createClient(String databaseName) throws IOException {
    if (clients == null) clients = new ArrayList<TDHSClient>();
    else clients.clear();
    int clientNumber = conf.getInt("database.executor.client.number", 1);
    new MysqlServerController().setMysqlDefaultConf(conf);
    String host = MysqlServerController.getMysqlConf(conf, "mysqld.bind-address", "localhost");
    host = host.replace("0.0.0.0", "localhost");
    int mysqlPort = Integer.valueOf(MysqlServerController.getMysqlConf(conf, "mysqld.port", 40001));
    String port = MysqlServerController.getMysqlConf(conf, "mysqld.tdh_socket_listen_port", mysqlPort + 3);
    int connectionPoolSize = conf.getInt("database.executor.connection.pool.size", 1);
    long timeout = conf.getLong("database.executor.timeout", 60000L);
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerHost=", host);
    Utilities.logInfo(logger, "create DatabaseExecutor with mysqlServerPort=", port);
    Utilities.logInfo(logger, "create DatabaseExecutor with Port=", port);
    Utilities.logInfo(logger, "create DatabaseExecutor with connectionPoolSize=", connectionPoolSize);
    Utilities.logInfo(logger, "create DatabaseExecutor with timeout=", timeout);
    try {
      for (int i = 0; i < clientNumber; ++i) {
        TDHSClient client =
            new TDHSClientImpl(new InetSocketAddress(host, Integer.valueOf(port)), connectionPoolSize, Long.valueOf(
                timeout).intValue());
        clients.add(client);
      }
    } catch (TDHSException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean open(DistributedDataRepositoryBaseOnTable repository, String dbname, String tableName, String[] columns)
      throws IOException {
    if (clients == null || clients.isEmpty()) {
      createClient(dbname);
      Utilities.logInfo(repository.getLogger(), "create clients with number=" + clients.size());
    }
    return true;
  }

  private TDHSCommon.FindFlag convertFindFlag(FindOperator operator) {
    switch (operator) {
    case EQ:
      return TDHSCommon.FindFlag.TDHS_EQ;
    case GT:
      return TDHSCommon.FindFlag.TDHS_GT;
    case GE:
      return TDHSCommon.FindFlag.TDHS_GE;
    case LE:
      return TDHSCommon.FindFlag.TDHS_LE;
    case LT:
      return TDHSCommon.FindFlag.TDHS_LT;
    }
    return null;
  }

  @Override
  public ResultSet findInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values,
      FindOperator operator, int limit, int offset) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());
      TDHSResponse response =
          client.get(repository.databaseName, repository.tableName, indexName, repository.tableColumns,
              new String[][] { values }, convertFindFlag(operator), offset, limit, null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(values) + ", operator=" + operator + ", TDHSResponse:"
          + response.toString()); }
      return response.getResultSet();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values) + ", operator="
          + operator, t);
    }
  }

  @Override
  public void insertInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values)
      throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());
      TDHSResponse response =
          client.insert(repository.databaseName, repository.tableName, repository.tableColumns, values);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(values) + ", TDHSResponse:" + response.toString()); }
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values), t);
    }
  }

  @Override
  public void updateInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      String[] values, FindOperator operator, int limit) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());

      List<ValueEntry> valueEntries = new ArrayList<ValueEntry>(values.length);
      for (String value : values) {
        valueEntries.add(new ValueEntry(TDHSCommon.UpdateFlag.TDHS_UPDATE_SET, value));
      }
      TDHSResponse response =
          client.update(repository.databaseName, repository.tableName, indexName, repository.tableColumns, valueEntries
              .toArray(new ValueEntry[values.length]), new String[][] { keys }, convertFindFlag(operator), 0, limit,
              null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator + ", TDHSResponse:"
          + response.toString()); }
    } catch (Throwable t) {
      throw new IOException(
          "indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator, t);
    }
  }

  @Override
  public void deleteInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      FindOperator operator, int limit) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());

      TDHSResponse response =
          client.delete(repository.databaseName, repository.tableName, indexName, new String[][] { keys },
              convertFindFlag(operator), 0, limit, null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator + ", TDHSResponse:"
          + response.toString()); }
    } catch (Throwable t) {
      throw new IOException(
          "indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", operator=" + operator, t);
    }
  }

  @Override
  public void close() throws IOException {
    if (clients != null) {
      for (TDHSClient client : clients) {
        if (client != null) client.shutdown();
      }
      clients.clear();
      clients = null;
    }
  }

  @Override
  public int getClientNumber() {
    return clients.size();
  }
}
