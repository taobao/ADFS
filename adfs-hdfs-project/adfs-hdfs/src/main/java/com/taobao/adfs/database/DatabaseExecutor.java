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
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.TableDescription;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DatabaseExecutor {
  public static final Logger logger = LoggerFactory.getLogger(DatabaseExecutor.class);
  private Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();

  public static enum Comparator {
    EQ("="), LT("<"), LE("<="), GT(">"), GE(">=");
    protected final String string;

    private Comparator(String string) {
      this.string = string;
    }

    public String toString() {
      return string;
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

  public ResultSet find(TableDescription tableDescripion, String indexName, String[] values, Comparator comparator,
      int limit, int offset) throws IOException {
    long startTime = System.currentTimeMillis();
    ResultSet resultSet = findInternal(tableDescripion, indexName, values, comparator, limit, offset);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(tableDescripion, "find", indexName, elapsedTime);
    Utilities.logDebug(logger, "spend ", elapsedTime, "ms to do ", getClass().getSimpleName(), ".",
        tableDescripion.rowClass.getSimpleName(), ".find.", indexName, ": values=", Utilities.deepToString(values),
        ", comparator=", comparator, ", limit=", limit, ", offset=", limit);
    return resultSet;
  }

  public void insert(TableDescription tableDescripion, String indexName, String[] values) throws IOException {
    long startTime = System.currentTimeMillis();
    AtomicLong counter = getCounter(tableDescripion);
    try {
      insertInternal(tableDescripion, indexName, values);
      counter.incrementAndGet();
    } catch (Throwable t) {
      counter.set(-1);
      throw new IOException(t);
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(tableDescripion, "insert", indexName, elapsedTime);
    Utilities.logDebug(logger, "spend ", elapsedTime, "ms to do ", getClass().getSimpleName(), ".",
        tableDescripion.rowClass.getSimpleName(), ".insert.", indexName, ": values=", Utilities.deepToString(values));
  }

  public void update(TableDescription tableDescripion, String indexName, String[] keys, String[] values,
      Comparator comparator, int limit) throws IOException {
    long startTime = System.currentTimeMillis();
    updateInternal(tableDescripion, indexName, keys, values, comparator, limit);
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(tableDescripion, "update", indexName, elapsedTime);
    Utilities.logDebug(logger, "spend ", elapsedTime, "ms to do ", getClass().getSimpleName(), ".",
        tableDescripion.rowClass.getSimpleName(), ".update.", indexName, ": keys=", Utilities.deepToString(keys),
        ", values=", Utilities.deepToString(values), ", comparator=", comparator, ", limit=", limit);
  }

  public void delete(TableDescription tableDescripion, String indexName, String[] keys, Comparator comparator, int limit)
      throws IOException {
    long startTime = System.currentTimeMillis();
    AtomicLong counter = getCounter(tableDescripion);
    try {
      deleteInternal(tableDescripion, indexName, keys, comparator, limit);
      counter.decrementAndGet();
    } catch (Throwable t) {
      counter.set(-1);
      throw new IOException(t);
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(tableDescripion, "delete", indexName, elapsedTime);
    Utilities.logDebug(logger, "spend ", elapsedTime, "ms to do ", getClass().getSimpleName(), ".",
        tableDescripion.rowClass.getSimpleName(), ".delete.", indexName, ": keys=", Utilities.deepToString(keys),
        ", comparator=", comparator, ", limit=", limit);
  }

  public long count(TableDescription tableDescripion) throws IOException {
    long startTime = System.currentTimeMillis();
    AtomicLong counter = getCounter(tableDescripion);
    if (counter.get() < 0) counter.set(countInternal(tableDescripion));
    long elapsedTime = System.currentTimeMillis() - startTime;
    updateMetrics(tableDescripion, "count", "PRIMARY", elapsedTime);
    Utilities.logDebug(logger, "spend ", elapsedTime, "ms to do ", getClass().getSimpleName(), ".",
        tableDescripion.rowClass.getSimpleName(), ".count");
    return counter.get();
  }

  private AtomicLong getCounter(TableDescription tableDescripion) {
    String counterName = tableDescripion.databaseName + "." + tableDescripion.tableName;
    AtomicLong counter = counters.get(counterName);
    if (counter == null) counters.put(counterName, counter = new AtomicLong(-1));
    return counter;
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

  void updateMetrics(TableDescription tableDescripion, String methodName, String indexName, long elapsedTime) {
    StringBuilder metricsName = new StringBuilder();
    String rowClassSimpleName = tableDescripion.rowClass.getSimpleName();
    metricsName.append("database.").append(methodName).append(rowClassSimpleName).append("By").append(indexName);
    DistributedMetrics.timeVaryingRateInc(metricsName.toString(), elapsedTime);
  }

  abstract public boolean open(TableDescription tableDescripion) throws IOException;

  abstract public ResultSet findInternal(TableDescription tableDescripion, String indexName, String[] values,
      Comparator comparator, int limit, int offset) throws IOException;

  abstract public void insertInternal(TableDescription tableDescripion, String indexName, String[] values)
      throws IOException;

  abstract public void updateInternal(TableDescription tableDescripion, String indexName, String[] keys,
      String[] values, Comparator comparator, int limit) throws IOException;

  abstract public void deleteInternal(TableDescription tableDescripion, String indexName, String[] keys,
      Comparator comparator, int limit) throws IOException;

  abstract public long countInternal(TableDescription tableDescripion) throws IOException;

  abstract public void close() throws IOException;

  abstract public int getClientNumber();

}