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

package com.taobao.adfs.distributed;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.database.DatabaseExecutor.Comparator;
import com.taobao.adfs.distributed.DistributedDataCache.CacheValue;
import com.taobao.adfs.distributed.DistributedOperation.DistributedOperand;
import com.taobao.adfs.distributed.DistributedOperation.DistributedOperator;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.AutoWritable;
import com.taobao.adfs.util.Utilities;
import com.taobao.adfs.util.Utilities.IgnoreToString;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DistributedDataRepositoryBaseOnTable {
  public Configuration conf = null;
  protected DistributedDataBaseOnDatabase distributedData = null;
  protected DistributedLocker locker = null;
  protected DistributedLockerForWriteRead readWriteLocker = null;
  protected DistributedDataCache cache = null;
  public CheckThread checkThread = null;

  protected DistributedDataRepositoryBaseOnTable(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  protected void initialize() throws IOException {
    DistributedDataRepositoryRow.parseTableDescription(getRowClass(), conf.get("mysql.server.engine", "InnoDB"));
  }

  public TableDescription getTableDescripion() {
    return DistributedDataRepositoryRow.getTableDescripion(getRowClass());
  }

  public DistributedDataVersion getVersion() {
    return distributedData.version;
  }

  abstract public Class<? extends DistributedDataRepositoryRow> getRowClass();

  public Logger getLogger() {
    return LoggerFactory.getLogger(getClass());
  }

  synchronized public void open(DistributedDataBaseOnDatabase distributedData) throws IOException {
    try {
      // DistributedDataVersion version, DatabaseExecutor hsClientExecutor,
      if (checkThread != null) checkThread.close();
      this.distributedData = distributedData;
      distributedData.databaseExecutor.open(getTableDescripion());
      cache = getCache();
      locker = new DistributedLocker();
      readWriteLocker = new DistributedLockerForWriteRead();
      checkThread = new CheckThread(getClass().getSimpleName());
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  synchronized public void close() throws IOException {
    if (checkThread != null) {
      checkThread.close();
      checkThread = null;
    }
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (locker != null) {
      locker.close();
      locker = null;
    }
    if (readWriteLocker != null) {
      readWriteLocker.close();
      readWriteLocker = null;
    }
  }

  synchronized public void format() throws IOException {
    try {
      clear();
      createMeta();
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  protected void createMeta() throws IOException {
  }

  abstract protected DistributedDataCache getCache() throws IOException;

  protected void clear() throws IOException {
    long maxVersion = findVersionFromData();
    for (long i = maxVersion; i >= 0; --i) {
      DistributedDataRepositoryRow row = findByVersion(i);
      if (row != null) {
        locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
        try {
          deletePhysically(row);
        } finally {
          locker.unlock(null, row.getKey());
        }
      }
      row = findByVersion(-i);
      if (row != null) {
        locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
        try {
          deletePhysically(row);
        } finally {
          locker.unlock(null, row.getKey());
        }
      }
    }
  }

  public List<?> findByVersionGreaterThan(long version, int limit) throws IOException {
    return find("VERSION", new Object[] { version }, Comparator.GT, limit);
  }

  public List<?> findByVersionLessThan(long version, int limit) throws IOException {
    return find("VERSION", new Object[] { version }, Comparator.LT, limit);
  }

  public DistributedDataRepositoryRow findByVersion(long version) throws IOException {
    List<?> rows = find("VERSION", new Object[] { version }, Comparator.EQ, 1);
    if (rows.isEmpty()) return null;
    return rows.isEmpty() ? null : (DistributedDataRepositoryRow) rows.get(0);
  }

  public long findVersionFromData() throws IOException {
    long versionStart = 0;
    long versionEnd = Long.MAX_VALUE;
    while (versionStart < versionEnd) {
      long versionMiddle = (versionStart + versionEnd) / 2;
      if (findByVersionGreaterThan(versionMiddle, 1).isEmpty()) versionEnd = versionMiddle;
      else versionStart = versionMiddle + 1;
    }
    long versionMax = versionEnd;

    versionStart = Long.MIN_VALUE;
    versionEnd = 0;
    while (versionStart < versionEnd) {
      long versionMiddle = (versionStart + versionEnd) / 2;
      if (findByVersionLessThan(versionMiddle, 1).isEmpty()) versionStart = versionMiddle;
      else versionEnd = versionMiddle - 1;
    }
    long versionMin = versionEnd;

    Utilities.logDebug(getLogger(), "get data version form database, version=", versionMax);
    return versionMax > -versionMin ? versionMax : -versionMin;
  }

  public long count() throws IOException {
    return distributedData.databaseExecutor.count(getTableDescripion());
  }

  public List<?> find(String indexName, Object[] keys, Comparator comparator, int limit) throws IOException {
    return find(indexName, keys, comparator, limit, true);
  }

  public List<?> find(String indexName, Object[] keys, Comparator comparator, int limit, boolean updateCacheMetrics)
      throws IOException {
    long startTime = System.currentTimeMillis();
    if (getCache() == null || getCache().getCapacity() <= 0) {
      List<?> resultRows = findInternal(indexName, keys, comparator, limit);
      updateMetrics("dataRepository.find" + getRowClass().getSimpleName() + "By" + indexName, startTime);
      return resultRows;
    }

    CacheValue cacheValue = getCache().get(updateCacheMetrics, indexName, keys);
    if (cacheValue != null) {
      List<DistributedDataRepositoryRow> resultRows = new ArrayList<DistributedDataRepositoryRow>();
      if (cacheValue.getValue() != null) resultRows.add((DistributedDataRepositoryRow) cacheValue.getValue());
      return resultRows;
    } else {
      boolean isLocked = getCache().lockForFind(indexName, keys);
      try {
        List<?> resultRows = findInternal(indexName, keys, comparator, limit);
        if (isLocked) getCache().addForFind(indexName, keys, resultRows);
        return resultRows;
      } finally {
        if (isLocked) getCache().unlockForFind(indexName, keys);
        updateMetrics("dataRepository.find" + getRowClass().getSimpleName() + "By" + indexName, startTime);
      }
    }
  }

  public DistributedDataRepositoryRow findOldRowForInsert(DistributedDataRepositoryRow row) throws IOException {
    return findByKeys(row.getKey());
  }

  public DistributedDataRepositoryRow insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow resultRow = insertInternal(row, overwrite);
    updateMetrics("dataRepository.insert" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  public DistributedDataRepositoryRow update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow resultRow = updateInternal(row, fieldsIndication);
    updateMetrics("dataRepository.update" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  public DistributedDataRepositoryRow delete(DistributedDataRepositoryRow row) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow resultRow = deleteInternal(row);
    updateMetrics("dataRepository.delete" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  final protected List<?> findInternal(String indexName, Object[] keys, Comparator comparator, int limit)
      throws IOException {
    // get from database
    String[] stringKeys = new String[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      stringKeys[i] = (keys[i] == null) ? null : keys[i].toString();
    }
    ResultSet resultSet = null;
    try {
      resultSet =
          distributedData.databaseExecutor.find(getTableDescripion(), indexName, stringKeys, comparator, limit, 0);
    } catch (Throwable t) {
      throw new DistributedException(true, "", t);
    }

    // parse rows from result set
    try {
      List<DistributedDataRepositoryRow> rows = new ArrayList<DistributedDataRepositoryRow>();
      while (resultSet.next()) {
        DistributedDataRepositoryRow row = getRowClass().newInstance();
        row.readFields(resultSet);
        rows.add(row);
      }
      resultSet.close();
      return rows;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  protected DistributedDataRepositoryRow insertInternal(DistributedDataRepositoryRow row, boolean overwrite)
      throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    try {
      (row = row.clone()).setVersion(distributedData.version.increaseAndGet());
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKey());
      if (oldRow != null && oldRow.getVersion() >= 0 && !overwrite)
        throw new IOException("fail to insert " + row + ": exsited " + oldRow);
      row.setIdentifier();
      if (oldRow != null) return updatePhysically(oldRow, row);
      else return insertPhysically(row);
    } finally {
      locker.unlock(null, row.getKey());
    }
  }

  protected DistributedDataRepositoryRow updateInternal(DistributedDataRepositoryRow row, int fieldsIndication)
      throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKey());
      if (oldRow == null || oldRow.getVersion() < 0) throw new IOException(row + ": not existed, oldRow=" + oldRow);
      row = oldRow.clone().update(row, fieldsIndication);
      row.setVersion(distributedData.version.increaseAndGet());
      row.setIdentifier();
      return updatePhysically(oldRow, row);
    } finally {
      locker.unlock(null, row.getKey());
    }
  }

  protected DistributedDataRepositoryRow deleteInternal(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKey());
      if (oldRow == null) return null;
      row = oldRow.clone();
      row.setVersion(-distributedData.version.increaseAndGet());
      return deletePhysically(row);
    } finally {
      locker.unlock(null, row.getKey());
    }
  }

  public DistributedDataRepositoryRow insertPhysically(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("row is null");
    if (getCache() == null || getCache().getCapacity() <= 0) {
      try {
        distributedData.databaseExecutor.insert(getTableDescripion(), "PRIMARY", row.getValueStrings());
        distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.INSERT, row));
        return row;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
      }
    }

    getCache().lockForInsert(row);
    try {
      distributedData.databaseExecutor.insert(getTableDescripion(), "PRIMARY", row.getValueStrings());
      distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.INSERT, row));
      getCache().addForInsert(row);
      return row;
    } catch (Throwable t) {
      // maybe this row has been inserted but network fails
      getCache().remove(row);
      throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
    } finally {
      getCache().unlockForInsert(row);
    }
  }

  public DistributedDataRepositoryRow updatePhysically(DistributedDataRepositoryRow oldRow,
      DistributedDataRepositoryRow newRow) throws IOException {
    if (newRow == null) throw new IOException("newRow is null");
    if (getCache() == null || getCache().getCapacity() <= 0) {
      try {
        distributedData.databaseExecutor.update(getTableDescripion(), "PRIMARY", newRow.getKeyStrings(), newRow
            .getValueStrings(), Comparator.EQ, 1);
        distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.UPDATE, newRow));
        return newRow;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + newRow + ", repository=" + toString(), t);
      }
    }

    if (oldRow == null) {
      oldRow = findByKeys(newRow.getKey());
      if (oldRow == null) throw new IOException("oldRow is null");
    }
    getCache().lockForUpdate(oldRow, newRow);
    try {
      distributedData.databaseExecutor.update(getTableDescripion(), "PRIMARY", newRow.getKeyStrings(), newRow
          .getValueStrings(), Comparator.EQ, 1);
      distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.UPDATE, newRow));
      getCache().addForUpdate(oldRow, newRow);
      return newRow;
    } catch (Throwable t) {
      // maybe this row has been updated but network fails
      getCache().remove(oldRow, newRow);
      throw new DistributedException(true, "oldRow=" + oldRow + ", newRow=" + newRow + ", repository=" + toString(), t);
    } finally {
      getCache().unlockForUpdate(oldRow, newRow);
    }
  }

  public DistributedDataRepositoryRow deletePhysically(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("row is null");
    if (getCache() == null || getCache().getCapacity() <= 0) {
      try {
        distributedData.databaseExecutor.delete(getTableDescripion(), "PRIMARY", row.getKeyStrings(), Comparator.EQ, 1);
        distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.DELETE, row));
        return row;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
      }
    }

    getCache().lockForDelete(row);
    try {
      distributedData.databaseExecutor.delete(getTableDescripion(), "PRIMARY", row.getKeyStrings(), Comparator.EQ, 1);
      distributedData.getOperationQueue().add(new DistributedOperation(DistributedOperator.DELETE, row));
      getCache().addForDelete(row);
      return row;
    } catch (Throwable t) {
      // maybe this row has been deleted but network fails
      getCache().remove(row);
      throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
    } finally {
      getCache().unlockForDelete(row);
    }
  }

  public DistributedDataRepositoryRow invokeDirectly(DistributedOperation operation) throws IOException {
    if (operation == null) throw new IOException("operation is null");
    if (operation.getOperand() == null) throw new IOException("operand is null");
    if (!(operation.getOperand() instanceof DistributedDataRepositoryRow))
      throw new IOException("operand type cannot be " + operation.operand);
    DistributedDataRepositoryRow row = (DistributedDataRepositoryRow) operation.getOperand();
    switch (operation.operator) {
    case INSERT:
      return insertDirectly(row);
    case UPDATE:
      return updateDirectly(row);
    case DELETE:
      return deleteDirectly(row);
    default:
      throw new IOException("unsupported DistributedOperator." + operation.operator);
    }
  }

  public DistributedDataRepositoryRow insertDirectly(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) return null;
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKey());
      if (oldRow == null) return insertPhysically(row);
      else if (Math.abs(oldRow.getVersion()) <= Math.abs(row.getVersion())) return updatePhysically(oldRow, row);
      else if (oldRow.getVersion() < 0) return deletePhysically(row);
      else return row;
    } finally {
      distributedData.version.greaterAndSet(Math.abs(row.getVersion()));
      locker.unlock(null, row.getKey());
    }
  }

  public DistributedDataRepositoryRow updateDirectly(DistributedDataRepositoryRow row) throws IOException {
    return insertDirectly(row);
  }

  public DistributedDataRepositoryRow deleteDirectly(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) return null;
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKey());
      // insert->delete===>delete->insert
      if (oldRow == null) return insertPhysically(row);
      else if (Math.abs(oldRow.getVersion()) <= Math.abs(row.getVersion())) return deletePhysically(row);
      else return row;
    } finally {
      distributedData.version.greaterAndSet(Math.abs(row.getVersion()));
      locker.unlock(null, row.getKey());
    }
  }

  public DistributedDataRepositoryRow findByKeys(Object[] keys) throws IOException {
    return findByKeys(keys, true);
  }

  public DistributedDataRepositoryRow findByKeys(Object[] keys, boolean updateCacheMetrics) throws IOException {
    List<?> rows = find("PRIMARY", keys, Comparator.EQ, 1, updateCacheMetrics);
    return rows.isEmpty() ? null : (DistributedDataRepositoryRow) rows.get(0);
  }

  protected List<?> removeDeletedRows(List<?> rows) {
    if (rows == null) return rows;
    List<Object> copyOfRows = new ArrayList<Object>(rows);
    for (Object row : copyOfRows) {
      if (((DistributedDataRepositoryRow) row).getVersion() < 0) rows.remove(row);
    }
    return rows;
  }

  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(getClass().getSimpleName()).append(":\n");
    stringBuilder.append("version=").append(distributedData.version == null ? "null" : distributedData.version.get())
        .append("\n");
    stringBuilder.append(locker == null ? "locker=null\n" : locker.toString()).append("\n");
    stringBuilder.append(cache == null ? "locker=null\n" : cache.toString());
    return stringBuilder.toString();
  }

  void updateMetrics(String metricsName, long startTime) {
    DistributedMetrics.timeVaryingRateInc(metricsName, System.currentTimeMillis() - startTime);
  }

  public abstract boolean isValid();

  protected void checkThreadTasks() {
    Utilities.sleepAndProcessInterruptedException(10, getLogger());
  }

  public class CheckThread extends Thread {
    volatile boolean shouldShutdown = false;

    CheckThread(String name) {
      setName(getClass().getSimpleName() + "-" + name + "@" + getName());
      setDaemon(true);
      start();
    }

    @Override
    public void run() {
      while (true) {
        try {
          if (shouldShutdown == true) break;
          checkThreadTasks();
        } catch (Throwable t) {
          Utilities.logWarn(getLogger(), "found error in check thread ", t);
        }
      }
    }

    public void close() {
      shouldShutdown = true;
      while (isAlive()) {
        Utilities.sleepAndProcessInterruptedException(1, getLogger());
      }
    }
  }

  /**
   * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
   */
  static abstract public class DistributedDataRepositoryRow extends DistributedOperand implements AutoWritable,
      com.taobao.adfs.util.Cloneable {
    @Column(indexes = { @Index(name = "VERSION", unique = true) })
    public long version = 0;
    static Map<Class<?>, Field[]> keyFieldsMap = new HashMap<Class<?>, Field[]>();
    @IgnoreToString
    Object[] key = null;
    @IgnoreToString
    String[] keyStrings = null;
    @IgnoreToString
    String[] valueStrings = null;

    static public final int ALL = -1;
    static public final int NONE = 0;
    static final public int VERSION = Integer.MIN_VALUE;

    public Object[] getKey() {
      Field[] keyFields = keyFieldsMap.get(getClass());
      if (keyFields == null) {
        synchronized (DistributedDataRepositoryRow.class) {
          keyFields = keyFieldsMap.get(getClass());
          if (keyFields == null) {
            TableDescription tableDescripion = tableDescriptions.get(getClass());
            List<Index> indexList = tableDescripion == null ? null : tableDescripion.indexMap.get("PRIMARY");
            keyFields = new Field[indexList == null ? 0 : indexList.size()];
            for (int i = 0; i < keyFields.length; ++i) {
              int indexOfIndex = indexList.get(i).index();
              keyFields[i] = tableDescripion.databaseFields.get(indexOfIndex);
            }
            keyFieldsMap.put(getClass(), keyFields);
          }
        }
      }
      if (key == null) key = new Object[keyFields.length];
      for (int i = 0; i < keyFields.length; ++i) {
        try {
          key[i] = keyFields[i].get(this);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
      return key;
    }

    public String[] getKeyStrings() throws IOException {
      Object[] keyValues = getKey();
      if (keyValues != null) {
        if (keyStrings == null) keyStrings = new String[keyValues.length];
        for (int i = 0; i < keyValues.length; ++i) {
          keyStrings[i] = keyValues[i] == null ? null : keyValues[i].toString();
        }
      }
      return keyStrings;
    }

    public String[] getValueStrings() throws IOException {
      TableDescription tableDescripion = tableDescriptions.get(getClass());
      List<Field> fieldList = tableDescripion == null ? null : tableDescripion.databaseFields;
      if (fieldList != null) {
        if (valueStrings == null) valueStrings = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); ++i) {
          try {
            Field field = fieldList.get(i);
            Object value = field.get(this);
            valueStrings[i] = value == null ? null : value.toString();
          } catch (Throwable t) {
            throw new IOException(t);
          }
        }
      }
      return valueStrings;
    }

    public long getVersion() {
      return version;
    }

    public long setVersion(long newVersion) {
      return version = newVersion;
    }

    abstract public DistributedDataRepositoryRow update(DistributedDataRepositoryRow row, int fieldsIndication);

    public DistributedDataRepositoryRow clone() {
      try {
        DistributedDataRepositoryRow row = getClass().newInstance();
        TableDescription tableDescripion = tableDescriptions.get(getClass());
        List<Field> fieldList = tableDescripion == null ? null : tableDescripion.databaseFields;
        if (fieldList != null) {
          for (int i = 0; i < fieldList.size(); ++i) {
            Field field = fieldList.get(i);
            Object value = field.get(this);
            field.set(row, value);
          }
        }
        row.clone(this);
        return row;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }

    public DistributedDataRepositoryRow readFields(ResultSet resultSet) throws IOException {
      TableDescription tableDescripion = tableDescriptions.get(getClass());
      if (resultSet != null && tableDescripion != null) {
        for (int i = 0; i < tableDescripion.databaseFields.size(); ++i) {
          try {
            Field field = tableDescripion.databaseFields.get(i);
            field.set(this, readResultSet(resultSet, i + 1, field.getType()));
          } catch (Throwable t) {
            throw new IOException(t);
          }
        }
      }
      return this;
    }

    public Object readResultSet(ResultSet resultSet, int fieldIndex, Class<?> resultType) throws SQLException,
        IOException {
      if (resultType == Boolean.TYPE || resultType == Boolean.class) {
        return resultSet.getBoolean(fieldIndex);
      } else if (resultType == Byte.TYPE || resultType == Byte.class) {
        return resultSet.getByte(fieldIndex);
      } else if (resultType == Short.TYPE || resultType == Short.class) {
        return resultSet.getShort(fieldIndex);
      } else if (resultType == Integer.TYPE || resultType == Integer.class) {
        return resultSet.getInt(fieldIndex);
      } else if (resultType == Long.TYPE || resultType == Long.class) {
        return resultSet.getLong(fieldIndex);
      } else if (resultType == Float.TYPE || resultType == Float.class) {
        return resultSet.getFloat(fieldIndex);
      } else if (resultType == Double.TYPE || resultType == Double.class) {
        return resultSet.getDouble(fieldIndex);
      } else if (resultType == String.class) {
        return resultSet.getString(fieldIndex);
      } else {
        throw new IOException("unsupportted type=" + resultType);
      }
    }

    public static Map<Class<?>, TableDescription> tableDescriptions = new HashMap<Class<?>, TableDescription>();

    public static TableDescription parseTableDescription(Class<?> rowClass, String mysqlServerEngine)
        throws IOException {
      TableDescription tableDescription = tableDescriptions.get(rowClass);
      if (tableDescription != null) return tableDescription;

      tableDescription = new TableDescription();
      tableDescription.rowClass = rowClass;
      Database database = rowClass.getAnnotation(Database.class);
      if (database != null && database.name() != null && !database.name().isEmpty()) tableDescription.databaseName =
          database.name();
      else tableDescription.databaseName = DistributedServer.class.getSimpleName().toLowerCase();
      Table table = rowClass.getAnnotation(Table.class);
      if (table != null && table.name() != null && !table.name().isEmpty()) tableDescription.tableName = table.name();
      else tableDescription.tableName = rowClass.getSimpleName().toLowerCase();

      StringBuilder tableSqlBuilder = new StringBuilder();
      tableSqlBuilder.append("CREATE DATABASE IF NOT EXISTS ").append(tableDescription.databaseName).append(
          " CHARACTER SET utf8;");
      tableSqlBuilder.append("USE ").append(tableDescription.databaseName).append(";");
      tableSqlBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableDescription.tableName).append("(");

      List<String> tableColumnList = new ArrayList<String>();
      List<Field> databaseFields = new ArrayList<Field>();
      for (Field field : Utilities.getFields(rowClass)) {
        Column column = field.getAnnotation(Column.class);
        if (column == null) continue;
        databaseFields.add(field);
        String columnName = column.name() == null || column.name().isEmpty() ? field.getName() : column.name();
        tableColumnList.add(columnName);
        String type = Utilities.getSqlTypeForJavaType(field.getType());
        tableSqlBuilder.append(columnName).append(" ").append(type).append("(").append(column.width()).append(")");
        if (column.binary()) tableSqlBuilder.append(" BINARY");
        if (!column.nullable()) tableSqlBuilder.append(" NOT NULL");
        if (!column.defaultValue().trim().equalsIgnoreCase("null") || column.nullable())
          tableSqlBuilder.append(" DEFAULT ").append(column.defaultValue());
        if (!column.signed()) tableSqlBuilder.append(" UNSIGNED");
        tableSqlBuilder.append(",");

        for (Index index : column.indexes()) {
          List<Index> indexList = tableDescription.indexMap.get(index.name());
          if (indexList == null) tableDescription.indexMap.put(index.name(), indexList = new ArrayList<Index>());
          List<String> indexListForColumnName = tableDescription.indexMapForColumnName.get(index.name());
          if (indexListForColumnName == null)
            tableDescription.indexMapForColumnName.put(index.name(), indexListForColumnName = new ArrayList<String>());
          indexList.add(index);
          indexListForColumnName.add(columnName);
          if (tableDescription.tableIndexes.get(index.name()) == null)
            tableDescription.tableIndexes.put(index.name(), tableDescription.tableIndexes.size());
        }
      }

      for (String indexName : tableDescription.indexMap.keySet()) {
        List<Index> indexList = tableDescription.indexMap.get(indexName);
        List<String> indexListForColumnName = tableDescription.indexMapForColumnName.get(indexName);
        if (indexList == null || indexList.isEmpty()) continue;
        if (indexList.get(0).name().equals("PRIMARY")) tableSqlBuilder.append("PRIMARY KEY");
        else if (indexList.get(0).unique()) tableSqlBuilder.append("UNIQUE KEY");
        else tableSqlBuilder.append("KEY");
        if (!indexList.get(0).name().equals("PRIMARY")) tableSqlBuilder.append(" ").append(indexList.get(0).name());
        tableSqlBuilder.append(" (");
        for (int i = 0; i < indexList.size(); ++i) {
          for (Index index : indexList) {
            if (index.index() != i) continue;
            tableSqlBuilder.append(indexListForColumnName.get(i));
            if (i < indexList.size() - 1) tableSqlBuilder.append(",");
          }
        }
        tableSqlBuilder.append("),");
      }
      if (tableSqlBuilder.toString().endsWith(","))
        tableSqlBuilder.delete(tableSqlBuilder.length() - 1, tableSqlBuilder.length());

      tableSqlBuilder.append(")ENGINE=" + mysqlServerEngine + ";");
      tableDescription.tableSql = tableSqlBuilder.toString();

      tableDescription.tableColumns = tableColumnList.toArray(new String[tableColumnList.size()]);
      tableDescription.databaseFields = databaseFields;
      tableDescriptions.put(rowClass, tableDescription);
      return tableDescription;
    }

    static public TableDescription getTableDescripion(Class<?> rowClass) {
      return tableDescriptions.get(rowClass);
    }

    @Override
    public String toString() {
      return Utilities.toStringByFields(this, true);
    }
  }

  static public class TableDescription {
    public Class<?> rowClass = null;
    public List<Field> databaseFields = null;
    public String tableSql = null;
    public String databaseName = null;
    public String tableName = null;
    public Map<String, Integer> tableIndexes = new HashMap<String, Integer>();
    public Map<String, List<Index>> indexMap = new HashMap<String, List<Index>>();
    public Map<String, List<String>> indexMapForColumnName = new HashMap<String, List<String>>();
    public String[] tableColumns = null;
  }

  @Retention(RUNTIME)
  public @interface Database {
    String name();
  }

  @Retention(RUNTIME)
  public @interface Table {
    String name() default "";
  }

  @Retention(RUNTIME)
  public @interface Column {
    String name() default "";

    int width() default 0;

    boolean signed() default true;

    boolean binary() default false;

    boolean nullable() default true;

    String defaultValue() default "NULL";

    Index[] indexes() default {};
  }

  @Retention(RUNTIME)
  public @interface Index {
    String name();

    int index() default 0;

    boolean unique() default false;

    int columnIndex() default 0;
  }
}