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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.code.hs4j.HSClient;
import com.taobao.adfs.database.handlersocket.HSClientSimulator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.TableDescription;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatabaseExecutorForHandlerSocketSimulator extends DatabaseExecutor {
  boolean independantRead = false;
  Configuration conf = null;
  List<HSClient> clients = null;
  Map<TableDescription, Integer> tableDescriptions = new HashMap<TableDescription, Integer>();

  public DatabaseExecutorForHandlerSocketSimulator(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
  }

  private void createClient(String databaseName) throws IOException {
    if (clients == null) clients = new ArrayList<HSClient>();
    else clients.clear();
    int clientNumber = conf.getInt("database.executor.client.number", 1);
    HSClient hsClient = new HSClientSimulator(conf);
    for (int i = 0; i < clientNumber; ++i) {
      clients.add(hsClient);
    }
  }

  int getIndexOffset(TableDescription tableDescription, String indexName, int hsClientIndex) {
    return clients.size() * (tableDescriptions.get(tableDescription) + tableDescription.tableIndexes.get(indexName))
        + hsClientIndex;
  }

  @Override
  public boolean open(TableDescription tableDescription) throws IOException {
    if (clients == null || clients.isEmpty()) {
      createClient(tableDescription.databaseName);
      Utilities.logInfo(logger, "create clients with number=" + clients.size());
    }

    int indexOffset = 0;
    for (TableDescription existedTableDescription : tableDescriptions.keySet()) {
      indexOffset += existedTableDescription.tableIndexes.size();
    }
    tableDescriptions.put(tableDescription, indexOffset);

    for (String indexName : tableDescription.tableIndexes.keySet()) {
      try {
        for (int i = 0; i < clients.size(); ++i) {
          int keyIndex = getIndexOffset(tableDescription, indexName, i);
          if (!clients.get(i).openIndex(keyIndex, tableDescription.databaseName, tableDescription.tableName, indexName,
              tableDescription.tableColumns))
            throw new IOException("fail to open index for handler socket simulator client");
        }
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }

    return true;
  }

  @Override
  public ResultSet findInternal(TableDescription tableDescription, String indexName, String[] values,
      Comparator comparator, int limit, int offset) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = clients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(tableDescription, indexName, currentHsCientIndex);
      return hsClient.find(indexSerialNumber, values, DatabaseExecutorForHandlerSocket.getOperator(comparator), limit,
          offset);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(values) + ", comparator="
          + comparator, t);
    }
  }

  @Override
  public void insertInternal(TableDescription tableDescription, String indexName, String[] values) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = clients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(tableDescription, indexName, currentHsCientIndex);
      boolean result = hsClient.insert(indexSerialNumber, values);
      if (!result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", values=" + Arrays.deepToString(values), t);
    }
  }

  @Override
  public void updateInternal(TableDescription tableDescription, String indexName, String[] keys, String[] values,
      Comparator comparator, int limit) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = clients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(tableDescription, indexName, currentHsCientIndex);
      int result =
          hsClient.update(indexSerialNumber, keys, values, DatabaseExecutorForHandlerSocket.getOperator(comparator),
              limit, 0);
      if (1 != result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", values="
          + Arrays.deepToString(values) + ", comparator=" + comparator, t);
    }
  }

  @Override
  public void deleteInternal(TableDescription tableDescription, String indexName, String[] keys, Comparator comparator,
      int limit) throws IOException {
    try {
      int currentHsCientIndex = getClientIndex();
      HSClient hsClient = clients.get(currentHsCientIndex);
      int indexSerialNumber = getIndexOffset(tableDescription, indexName, currentHsCientIndex);
      int result =
          hsClient.delete(indexSerialNumber, keys, DatabaseExecutorForHandlerSocket.getOperator(comparator), limit, 0);
      if (1 != result) throw new IOException("table is locked/readonly/up_to_capacity, result=" + result);
    } catch (Throwable t) {
      throw new IOException("indexName=" + indexName + ", keys=" + Arrays.deepToString(keys) + ", comparator="
          + comparator, t);
    }
  }

  public long countInternal(TableDescription tableDescripion) throws IOException {
    int currentHsCientIndex = getClientIndex();
    HSClientSimulator hsClient = (HSClientSimulator) clients.get(currentHsCientIndex);
    return hsClient.count(tableDescripion.tableName);
  }

  @Override
  public void close() throws IOException {
    if (clients != null) {
      for (HSClient hsClient : clients) {
        if (hsClient != null) hsClient.shutdown();
      }
      clients.clear();
      clients = null;
    }
    if (tableDescriptions != null) {
      tableDescriptions.clear();
      tableDescriptions = null;
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DatabaseExecutorForHandlerSocket=");
    if (clients == null || clients.isEmpty()) builder.append("no-client");
    else builder.append(clients.get(0));
    return builder.toString();
  }

  @Override
  public int getClientNumber() {
    return clients.size();
  }
}
