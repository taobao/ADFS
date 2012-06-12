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

package com.taobao.adfs.datanode;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.database.DatabaseExecutor.Comparator;
import com.taobao.adfs.distributed.DistributedDataCache;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DatanodeRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(DatanodeRepository.class);

  public DatanodeRepository(Configuration conf) throws IOException {
    super(conf);
  }

  public Datanode findById(long id) throws IOException {
    Datanode datanode = (Datanode) findByKeys(new Object[] { id });
    if (datanode != null && datanode.version >= 0) return datanode;
    return null;
  }

  public List<Datanode> findByIdGreateOrEqual(long id) throws IOException {
    List<Datanode> datanodes = find("PRIMARY", new Object[] { Long.MIN_VALUE }, Comparator.GE, Integer.MAX_VALUE);
    removeDeletedRows(datanodes);
    return datanodes;
  }

  public List<Datanode> findByStorageId(String storageId) throws IOException {
    List<Datanode> datanodes = find("STORAGE_ID", new Object[] { storageId }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(datanodes);
    return datanodes;
  }

  public List<Datanode> findByName(String name) throws IOException {
    List<Datanode> datanodes = find("NAME", new Object[] { name }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(datanodes);
    return datanodes;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return Datanode.class;
  }

  @SuppressWarnings("unchecked")
  public List<Datanode> find(String indexName, Object[] keys, Comparator comparator, int limit) throws IOException {
    return (List<Datanode>) super.find(indexName, keys, comparator, limit);
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
      findInternal("PRIMARY", new Object[] { 0 }, Comparator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  protected DistributedDataCache getCache() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
