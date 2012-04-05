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
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.hs4j.FindOperator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DatabaseExecutor {
  public static final Logger logger = LoggerFactory.getLogger(DatabaseExecutor.class);

  public static enum Comparator {
    EQ(0), LT(1), LE(2), GT(3), GE(4);
    protected final int value;

    private Comparator(int value) {
      this.value = value;
    }

    public int value() {
      return value;
    }
  }

  public static DatabaseExecutor get(Configuration conf) throws IOException {
    String clientClassName = null;
    try {
      clientClassName = conf.get("database.executor.class.name", DatabaseExecutorForMysqlClient.class.getName());
      int clientNumber = conf.getInt("database.executor.client.number", 1);
      if (clientNumber < 1) clientNumber = 1;
      Utilities.logInfo(logger, "create DatabaseExecutor with database.executor.class.name=", clientClassName);
      Utilities.logInfo(logger, "create DatabaseExecutor with database.executor.client.number=", clientNumber);
      @SuppressWarnings("unchecked")
      Class<DatabaseExecutor> clientClass = (Class<DatabaseExecutor>) Class.forName(clientClassName);
      Constructor<DatabaseExecutor> clientConstructor = clientClass.getConstructor(Configuration.class);
      return clientConstructor.newInstance(conf);
    } catch (Throwable t) {
      throw new IOException("fail to create DatabaseExecutor with database.executor.class.name=" + clientClassName, t);
    }
  }

  public static boolean needMysqlServer(Configuration conf) {
    if (conf == null) return false;
    String clientClassName = conf.get("database.executor.class.name", DatabaseExecutorForMysqlClient.class.getName());
    return !clientClassName.equals(DatabaseExecutorForHandlerSocketSimulator.class.getName());
  }

  Configuration conf = new Configuration(false);

  public ResultSet find(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values,
      FindOperator operator, int limit, int offset) throws IOException {
    long startTime = System.currentTimeMillis();
    ResultSet resultSet = findInternal(repository, indexName, values, operator, limit, offset);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(repository, "find", indexName, elapsedTime);
    long debugElapsedTime = conf.getLong("database.executor.debug.elapsed.time", 0);
    Utilities.log(logger, elapsedTime >= debugElapsedTime ? Level.DEBUG : Level.TRACE, "spend ", elapsedTime,
        "ms to do ", getClass().getSimpleName(), ".", repository.getClass().getSimpleName(), ".find.", indexName,
        ": values=", Utilities.deepToString(values), ", operator=", operator, ", limit=", limit, ", offset=", limit);
    return resultSet;
  }

  public void insert(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values)
      throws IOException {
    long startTime = System.currentTimeMillis();
    insertInternal(repository, indexName, values);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(repository, "insert", indexName, elapsedTime);
    long debugElapsedTime = conf.getLong("database.executor.debug.elapsed.time", 0);
    Utilities.log(logger, elapsedTime >= debugElapsedTime ? Level.DEBUG : Level.TRACE, "spend ", elapsedTime,
        "ms to do ", getClass().getSimpleName(), ".", repository.getClass().getSimpleName(), ".insert.", indexName,
        ": values=", Utilities.deepToString(values));
  }

  public void update(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys, String[] values,
      FindOperator operator, int limit) throws IOException {
    long startTime = System.currentTimeMillis();
    updateInternal(repository, indexName, keys, values, operator, limit);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(repository, "update", indexName, elapsedTime);
    long debugElapsedTime = conf.getLong("database.executor.debug.elapsed.time", 0);
    Utilities.log(logger, elapsedTime >= debugElapsedTime ? Level.DEBUG : Level.TRACE, "spend ", elapsedTime,
        "ms to do ", getClass().getSimpleName(), ".", repository.getClass().getSimpleName(), ".update.", indexName,
        ": keys=", Utilities.deepToString(keys), ", values=", Utilities.deepToString(values), ", operator=", operator,
        ", limit=", limit);
  }

  public void delete(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      FindOperator operator, int limit) throws IOException {
    long startTime = System.currentTimeMillis();
    deleteInternal(repository, indexName, keys, operator, limit);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(repository, "delete", indexName, elapsedTime);
    long debugElapsedTime = conf.getLong("database.executor.debug.elapsed.time", 0);
    Utilities.log(logger, elapsedTime >= debugElapsedTime ? Level.DEBUG : Level.TRACE, "spend ", elapsedTime,
        "ms to do ", getClass().getSimpleName(), ".", repository.getClass().getSimpleName(), ".delete.", indexName,
        ": keys=", Utilities.deepToString(keys), ", operator=", operator, ", limit=", limit);
  }

  protected ConcurrentHashMap<Long, Integer> threadRecorder = new ConcurrentHashMap<Long, Integer>();

  protected int getClientIndex() {
    long threadId = Thread.currentThread().getId();
    Integer threadIndex = threadRecorder.get(threadId);
    if (threadIndex == null) {
      synchronized (threadRecorder) {
        threadRecorder.put(threadId, threadIndex = threadRecorder.size());
      }
    }
    return threadIndex % getClientNumber();
  }

  void updateMetrics(DistributedDataRepositoryBaseOnTable repository, String methodName, String indexName,
      long elapsedTime) {
    StringBuilder metricsName = new StringBuilder();
    String rowClassSimpleName = repository.getRowClass().getSimpleName();
    metricsName.append("database.").append(methodName).append(rowClassSimpleName).append("By").append(indexName);
    DistributedMetrics.timeVaryingRateInc(metricsName.toString(), elapsedTime);
  }

  abstract public boolean open(DistributedDataRepositoryBaseOnTable repository, String dbname, String tableName,
      String[] columns) throws IOException;

  abstract public ResultSet findInternal(DistributedDataRepositoryBaseOnTable repository, String indexName,
      String[] values, FindOperator operator, int limit, int offset) throws IOException;

  abstract public void insertInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] values)
      throws IOException;

  abstract public void updateInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      String[] values, FindOperator operator, int limit) throws IOException;

  abstract public void deleteInternal(DistributedDataRepositoryBaseOnTable repository, String indexName, String[] keys,
      FindOperator operator, int limit) throws IOException;

  abstract public void close() throws IOException;

  abstract public int getClientNumber();

}