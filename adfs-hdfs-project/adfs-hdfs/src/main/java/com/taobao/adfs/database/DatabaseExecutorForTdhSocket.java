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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.taobao.adfs.database.tdhsocket.client.TDHSClient;
import com.taobao.adfs.database.tdhsocket.client.TDHSClientImpl;
import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.request.ValueEntry;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponseEnum;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.TableDescription;
import com.taobao.adfs.util.Utilities;

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
    int mysqlPort = Integer.valueOf(MysqlServerController.getMysqlConf(conf, "mysqld.port", 50001));
    String port = MysqlServerController.getMysqlConf(conf, "mysqld.tdh_socket_listen_port", mysqlPort + 3);
    int connectionPoolSize = conf.getInt("database.executor.connection.pool.size", 1);
    long timeout = conf.getLong("database.executor.timeout", 60000L);
    Utilities.logInfo(logger, "create DatabaseExecutor with class=", getClass().getName());
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
  public boolean open(TableDescription tableDescripion) throws IOException {
    if (clients == null || clients.isEmpty()) {
      createClient(tableDescripion.databaseName);
      Utilities.logInfo(logger, "create clients with number=" + clients.size());
    }
    return true;
  }

  static public TDHSCommon.FindFlag getOperator(Comparator comparator) {
    switch (comparator) {
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
    default:
      throw new RuntimeException("never come to here");
    }
  }

  @Override
  public ResultSet findInternal(TableDescription tableDescripion, String indexName, String[] values,
      Comparator comparator, int limit, int offset) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());
      TDHSResponse response =
          client.get(tableDescripion.databaseName, tableDescripion.tableName, indexName, tableDescripion.tableColumns,
              new String[][] { values }, getOperator(comparator), offset, limit, null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(values) + ", comparator=" + comparator + ", TDHSResponse:"
          + response.toString()); }
      return response.getResultSet();
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values) + ", comparator="
          + comparator, t);
    }
  }

  @Override
  public void insertInternal(TableDescription tableDescripion, String indexName, String[] values) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());
      TDHSResponse response =
          client.insert(tableDescripion.databaseName, tableDescripion.tableName, tableDescripion.tableColumns, values);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(values) + ", TDHSResponse:" + response.toString()); }
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values), t);
    }
  }

  @Override
  public void updateInternal(TableDescription tableDescripion, String indexName, String[] keys, String[] values,
      Comparator comparator, int limit) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());

      List<ValueEntry> valueEntries = new ArrayList<ValueEntry>(values.length);
      for (String value : values) {
        valueEntries.add(new ValueEntry(TDHSCommon.UpdateFlag.TDHS_UPDATE_SET, value));
      }
      TDHSResponse response =
          client.update(tableDescripion.databaseName, tableDescripion.tableName, indexName,
              tableDescripion.tableColumns, valueEntries.toArray(new ValueEntry[values.length]),
              new String[][] { keys }, getOperator(comparator), 0, limit, null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator=" + comparator + ", TDHSResponse:"
          + response.toString()); }
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator="
          + comparator, t);
    }
  }

  @Override
  public void deleteInternal(TableDescription tableDescripion, String indexName, String[] keys, Comparator comparator,
      int limit) throws IOException {
    try {
      TDHSClient client = clients.get(getClientIndex());

      TDHSResponse response =
          client.delete(tableDescripion.databaseName, tableDescripion.tableName, indexName, new String[][] { keys },
              getOperator(comparator), 0, limit, null);
      if (!TDHSResponseEnum.ClientStatus.OK.equals(response.getStatus())) { throw new IOException("indexName="
          + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator=" + comparator + ", TDHSResponse:"
          + response.toString()); }
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator="
          + comparator, t);
    }
  }

  public long countInternal(TableDescription tableDescripion) throws IOException {
    return -1;
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
