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

package com.taobao.adfs.datanode;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.hs4j.FindOperator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedException;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatanodeRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(DatanodeRepository.class);

  public DatanodeRepository(Configuration conf) throws IOException {
    super(conf);
  }

  public Datanode findById(int id) throws IOException {
    Datanode datanode = (Datanode) findByKeys(new Object[] { id });
    if (datanode != null && datanode.version >= 0) return datanode;
    return null;
  }

  public List<Datanode> findByUpdateTimeGreaterOrEqualThan(long updatedTime) throws IOException {
    List<Datanode> datanodes = find("LAST_UPDATED", new Object[] { updatedTime }, FindOperator.GE, Integer.MAX_VALUE);
    removeDeletedRows(datanodes);
    return datanodes;
  }

  public List<Datanode> findByStorageId(String storageId) throws IOException {
    List<Datanode> datanodes = find("STORAGE_ID", new Object[] { storageId }, FindOperator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(datanodes);
    return datanodes;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return Datanode.class;
  }

  @SuppressWarnings("unchecked")
  public List<Datanode> find(String indexName, Object[] keys, FindOperator findOperator, int limit) throws IOException {
    return (List<Datanode>) super.find(indexName, keys, findOperator, limit);
  }

  public Datanode insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    return (Datanode) super.insert(row, overwrite);
  }

  public Datanode update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    return (Datanode) super.update(row, fieldsIndication);
  }

  public Datanode delete(DistributedDataRepositoryRow row) throws IOException {
    return (Datanode) super.delete(row);
  }

  public boolean isValid() {
    try {
      findInternal("PRIMARY", new Object[] { 0 }, FindOperator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  protected List<Datanode> findInternal(String indexName, Object[] keys, FindOperator findOperator, int limit)
      throws IOException {
    // get from database
    String[] stringKeys = new String[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      stringKeys[i] = (keys[i] == null) ? null : keys[i].toString();
    }
    ResultSet resultSet = null;
    try {
      resultSet = databaseExecutor.findInternal(this, indexName, stringKeys, findOperator, limit, 0);
    } catch (Throwable t) {
      throw new DistributedException(true, "", t);
    }

    try {
      List<Datanode> datanodes = new ArrayList<Datanode>();
      while (resultSet.next()) {
        // convert to datanode
        Datanode datanode = new Datanode();
        datanode.id = resultSet.getInt(1);
        datanode.name = resultSet.getString(2);
        datanode.storageId = resultSet.getString(3);
        datanode.ipcPort = resultSet.getInt(4);
        datanode.infoPort = resultSet.getInt(5);
        datanode.layoutVersion = resultSet.getInt(6);
        datanode.namespaceId = resultSet.getInt(7);
        datanode.ctime = resultSet.getLong(8);
        datanode.capacity = resultSet.getLong(9);
        datanode.dfsUsed = resultSet.getLong(10);
        datanode.remaining = resultSet.getLong(11);
        datanode.lastUpdated = resultSet.getLong(12);
        datanode.xceiverCount = resultSet.getInt(13);
        datanode.location = resultSet.getString(14);
        datanode.hostName = resultSet.getString(15);
        datanode.adminState = resultSet.getByte(16);
        datanode.version = resultSet.getLong(17);
        datanode.setOperateIdentifier(resultSet.getString(18));
        datanodes.add(datanode);
      }
      resultSet.close();
      return datanodes;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
