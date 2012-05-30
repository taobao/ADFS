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

package com.taobao.adfs.lease;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.database.DatabaseExecutor.Comparator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class LeaseRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(LeaseRepository.class);

  public LeaseRepository(Configuration conf) throws IOException {
    super(conf);
  }

  public Lease findByHolder(String holder) throws IOException {
    List<Lease> leaseList = find("PRIMARY", new Object[] { holder }, Comparator.EQ, Integer.MAX_VALUE);
    if (leaseList == null || leaseList.isEmpty() || leaseList.get(0).version < 0) return null;
    return leaseList.get(0);
  }

  public List<Lease> findByTimeLessThan(long time) throws IOException {
    List<Lease> leaseList = find("TIME", new Object[] { time }, Comparator.LT, Integer.MAX_VALUE);
    removeDeletedRows(leaseList);
    return leaseList;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return Lease.class;
  }

  @SuppressWarnings("unchecked")
  public List<Lease> find(String indexName, Object[] keys, Comparator comparator, int limit) throws IOException {
    return (List<Lease>) super.find(indexName, keys, comparator, limit);
  }

  public Lease insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    return (Lease) super.insert(row, overwrite);
  }

  public Lease update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    return (Lease) super.update(row, fieldsIndication);
  }

  public Lease delete(DistributedDataRepositoryRow row) throws IOException {
    return (Lease) super.delete(row);
  }

  public boolean isValid() {
    try {
      findInternal("PRIMARY", new Object[] { null }, Comparator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }
}
