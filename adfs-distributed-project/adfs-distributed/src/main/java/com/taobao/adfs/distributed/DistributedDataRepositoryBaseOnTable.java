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

package com.taobao.adfs.distributed;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.hs4j.FindOperator;
import com.taobao.adfs.database.DatabaseExecutor;
import com.taobao.adfs.distributed.DistributedDataCache.CacheValue;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.AutoWritable;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DistributedDataRepositoryBaseOnTable {
  public Configuration conf = null;
  protected DistributedDataVersion version = null;
  protected DistributedLocker locker = null;
  protected DistributedLockerForWriteRead readWriteLocker = null;
  protected DistributedDataCache cache = null;
  protected DatabaseExecutor databaseExecutor = null;
  public String databaseName = null;
  public String tableName = null;
  public String[] tableColumns = null;
  public Map<String, Integer> tableIndexes = new HashMap<String, Integer>();
  public Map<String, List<Index>> indexMap = new HashMap<String, List<Index>>();
  public Map<String, List<String>> indexMapForColumnName = new HashMap<String, List<String>>();
  protected String tableSql = null;
  public CheckThread checkThread = null;

  protected DistributedDataRepositoryBaseOnTable(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  protected void initialize() throws IOException {
    parseTableDescription();
  }

  void parseTableDescription() throws IOException {
    Class<?> rowClass = getRowClass();
    Database database = rowClass.getAnnotation(Database.class);
    if (database != null && database.name() != null && !database.name().isEmpty()) databaseName = database.name();
    else databaseName = DistributedServer.class.getSimpleName().toLowerCase();
    Table table = rowClass.getAnnotation(Table.class);
    if (table != null && table.name() != null && !table.name().isEmpty()) tableName = table.name();
    else tableName = rowClass.getSimpleName().toLowerCase();

    StringBuilder tableSqlBuilder = new StringBuilder();
    tableSqlBuilder.append("CREATE DATABASE IF NOT EXISTS ").append(databaseName).append(" CHARACTER SET utf8;");
    tableSqlBuilder.append("USE ").append(databaseName).append(";");
    tableSqlBuilder.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append("(");

    List<String> tableColumnList = new ArrayList<String>();
    for (Field field : Utilities.getFields(rowClass)) {
      Column column = field.getAnnotation(Column.class);
      if (column == null) continue;

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
        List<Index> indexList = indexMap.get(index.name());
        if (indexList == null) indexMap.put(index.name(), indexList = new ArrayList<Index>());
        List<String> indexListForColumnName = indexMapForColumnName.get(index.name());
        if (indexListForColumnName == null)
          indexMapForColumnName.put(index.name(), indexListForColumnName = new ArrayList<String>());
        indexList.add(index);
        indexListForColumnName.add(columnName);
        if (tableIndexes.get(index.name()) == null) tableIndexes.put(index.name(), tableIndexes.size());
      }
    }

    for (String indexName : indexMap.keySet()) {
      List<Index> indexList = indexMap.get(indexName);
      List<String> indexListForColumnName = indexMapForColumnName.get(indexName);
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

    String mysqlServerEngine = conf.get("mysql.server.engine", "InnoDB");
    tableSqlBuilder.append(")ENGINE=" + mysqlServerEngine + ";");
    tableSql = tableSqlBuilder.toString();

    tableColumns = tableColumnList.toArray(new String[tableColumnList.size()]);
  }

  public String getSql() {
    return tableSql;
  }

  public DistributedDataVersion getVersion() {
    return version;
  }

  abstract public Class<? extends DistributedDataRepositoryRow> getRowClass();

  public Logger getLogger() {
    return LoggerFactory.getLogger(getClass());
  }

  synchronized public void open(DistributedDataVersion version, DatabaseExecutor hsClientExecutor) throws IOException {
    try {
      if (checkThread != null) checkThread.close();
      this.version = version;
      this.databaseExecutor = hsClientExecutor;
      this.databaseExecutor.open(this, databaseName, tableName, tableColumns);
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
    version = null;
    databaseExecutor = null;
  }

  synchronized public List<Object> format() throws IOException {
    try {
      clear();
      return createMeta();
    } catch (IOException e) {
      close();
      throw e;
    }
  }

  protected List<Object> createMeta() throws IOException {
    return new ArrayList<Object>(0);
  }

  protected DistributedDataCache getCache() throws IOException {
    return null;
  }

  protected void clear() throws IOException {
    long maxVersion = findVersionFromData();
    for (long i = maxVersion; i >= 0; --i) {
      DistributedDataRepositoryRow row = findByVersion(i);
      if (row != null) {
        locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
        try {
          deletePhysically(row);
        } finally {
          locker.unlock(null, row.getKeys());
        }
      }
      row = findByVersion(-i);
      if (row != null) {
        locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
        try {
          deletePhysically(row);
        } finally {
          locker.unlock(null, row.getKeys());
        }
      }
    }
  }

  public List<?> findByVersionGreaterThan(long version, int limit) throws IOException {
    return find("VERSION", new Object[] { version }, FindOperator.GT, limit);
  }

  public List<?> findByVersionLessThan(long version, int limit) throws IOException {
    return find("VERSION", new Object[] { version }, FindOperator.LT, limit);
  }

  public DistributedDataRepositoryRow findByVersion(long version) throws IOException {
    List<?> rows = find("VERSION", new Object[] { version }, FindOperator.EQ, 1);
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

  public List<?> find(String indexName, Object[] keys, FindOperator findOperator, int limit) throws IOException {
    return find(indexName, keys, findOperator, limit, true);
  }

  public List<?> find(String indexName, Object[] keys, FindOperator findOperator, int limit, boolean updateCacheMetrics)
      throws IOException {
    long startTime = System.currentTimeMillis();
    if (getCache() == null || getCache().getCapacity() <= 0) {
      List<?> resultRows = findInternal(indexName, keys, findOperator, limit);
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
        List<?> resultRows = findInternal(indexName, keys, findOperator, limit);
        if (isLocked) getCache().addForFind(indexName, keys, resultRows);
        return resultRows;
      } finally {
        if (isLocked) getCache().unlockForFind(indexName, keys);
        updateMetrics("dataRepository.find" + getRowClass().getSimpleName() + "By" + indexName, startTime);
      }
    }
  }

  public DistributedDataRepositoryRow findOldRowForInsert(DistributedDataRepositoryRow row) throws IOException {
    return findByKeys(row.getKeys());
  }

  public DistributedDataRepositoryRow insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow oldRow = findOldRowForInsert(row);
    if (oldRow != null && oldRow.isOperateIdentifierMatched()) {
      oldRow.setNote("insert");
      return oldRow;
    }
    DistributedDataRepositoryRow resultRow = insertInternal(row, overwrite);
    updateMetrics("dataRepository.insert" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  public DistributedDataRepositoryRow update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
    if (oldRow != null && oldRow.isOperateIdentifierMatched()) {
      oldRow.setNote("update");
      return oldRow;
    }
    DistributedDataRepositoryRow resultRow = updateInternal(row, fieldsIndication);
    updateMetrics("dataRepository.update" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  public DistributedDataRepositoryRow delete(DistributedDataRepositoryRow row) throws IOException {
    long startTime = System.currentTimeMillis();
    DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
    if (oldRow != null && oldRow.isOperateIdentifierMatched()) {
      oldRow.setNote("delete");
      return oldRow;
    }
    DistributedDataRepositoryRow resultRow = deleteInternal(row);
    updateMetrics("dataRepository.delete" + getRowClass().getSimpleName(), startTime);
    return resultRow;
  }

  abstract protected List<?> findInternal(String indexName, Object[] keys, FindOperator findOperator, int limit)
      throws IOException;

  protected DistributedDataRepositoryRow insertInternal(DistributedDataRepositoryRow row, boolean overwrite)
      throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      (row = row.clone()).setVersion(version.increaseAndGet());
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
      if (oldRow != null && oldRow.getVersion() >= 0 && !overwrite)
        throw new IOException("fail to insert " + row + ": exsited " + oldRow);
      row.setOperateIdentifier();
      if (oldRow != null) return updatePhysically(oldRow, row);
      else return insertPhysically(row);
    } finally {
      locker.unlock(null, row.getKeys());
    }
  }

  protected DistributedDataRepositoryRow updateInternal(DistributedDataRepositoryRow row, int fieldsIndication)
      throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
      if (oldRow == null || oldRow.getVersion() < 0) throw new IOException(row + ": not existed, oldRow=" + oldRow);
      row = oldRow.clone().update(row, fieldsIndication);
      row.setVersion(version.increaseAndGet());
      row.setOperateIdentifier();
      return updatePhysically(oldRow, row);
    } finally {
      locker.unlock(null, row.getKeys());
    }
  }

  protected DistributedDataRepositoryRow deleteInternal(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("row is null");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
      if (oldRow == null) return null;
      row = oldRow.clone();
      row.setVersion(-version.increaseAndGet());
      return deletePhysically(row);
    } finally {
      locker.unlock(null, row.getKeys());
    }
  }

  public DistributedDataRepositoryRow insertPhysically(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("row is null");
    if (getCache() == null || getCache().getCapacity() <= 0) {
      try {
        databaseExecutor.insertInternal(this, "PRIMARY", row.getStringValues());
        row.setNote("insert");
        return row;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
      }
    }

    getCache().lockForInsert(row);
    try {
      databaseExecutor.insertInternal(this, "PRIMARY", row.getStringValues());
      row.setNote(null);
      getCache().addForInsert(row);
      row.setNote("insert");
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
        databaseExecutor.updateInternal(this, "PRIMARY", newRow.getStringKeys(), newRow.getStringValues(),
            FindOperator.EQ, 1);
        newRow.setNote("update");
        return newRow;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + newRow + ", repository=" + toString(), t);
      }
    }

    if (oldRow == null) {
      oldRow = findByKeys(newRow.getKeys());
      if (oldRow == null) throw new IOException("oldRow is null");
    }
    getCache().lockForUpdate(oldRow, newRow);
    try {
      databaseExecutor.updateInternal(this, "PRIMARY", newRow.getStringKeys(), newRow.getStringValues(),
          FindOperator.EQ, 1);
      newRow.setNote(null);
      getCache().addForUpdate(oldRow, newRow);
      newRow.setNote("update");
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
        databaseExecutor.deleteInternal(this, "PRIMARY", row.getStringKeys(), FindOperator.EQ, 1);
        row.setNote("delete");
        return row;
      } catch (Throwable t) {
        throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
      }
    }

    getCache().lockForDelete(row);
    try {
      databaseExecutor.deleteInternal(this, "PRIMARY", row.getStringKeys(), FindOperator.EQ, 1);
      row.setNote(null);
      getCache().addForDelete(row);
      row.setNote("delete");
      return row;
    } catch (Throwable t) {
      // maybe this row has been deleted but network fails
      getCache().remove(row);
      throw new DistributedException(true, "row=" + row + ", repository=" + toString(), t);
    } finally {
      getCache().unlockForDelete(row);
    }
  }

  public DistributedDataRepositoryRow insertDirectly(DistributedDataRepositoryRow row) throws IOException {
    if (row == null || row.getNote() == null) return null;
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
      if (oldRow == null) return insertPhysically(row);
      else if (Math.abs(oldRow.getVersion()) <= Math.abs(row.getVersion())) return updatePhysically(oldRow, row);
      else if (oldRow.getVersion() < 0) return deletePhysically(row);
      else return row;
    } finally {
      version.greaterAndSet(Math.abs(row.getVersion()));
      locker.unlock(null, row.getKeys());
    }
  }

  public DistributedDataRepositoryRow updateDirectly(DistributedDataRepositoryRow row) throws IOException {
    return insertDirectly(row);
  }

  public DistributedDataRepositoryRow deleteDirectly(DistributedDataRepositoryRow row) throws IOException {
    if (row == null || row.getNote() == null) return null;
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      DistributedDataRepositoryRow oldRow = findByKeys(row.getKeys());
      // insert->delete===>delete->insert
      if (oldRow == null) return insertPhysically(row);
      else if (Math.abs(oldRow.getVersion()) <= Math.abs(row.getVersion())) return deletePhysically(row);
      else return row;
    } finally {
      version.greaterAndSet(Math.abs(row.getVersion()));
      locker.unlock(null, row.getKeys());
    }
  }

  public DistributedDataRepositoryRow findByKeys(Object[] keys) throws IOException {
    return findByKeys(keys, true);
  }

  public DistributedDataRepositoryRow findByKeys(Object[] keys, boolean updateCacheMetrics) throws IOException {
    List<?> rows = find("PRIMARY", keys, FindOperator.EQ, 1, updateCacheMetrics);
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
    stringBuilder.append("version=").append(version == null ? "null" : version.get()).append("\n");
    stringBuilder.append(databaseExecutor == null ? "locker=null\n" : locker.toString()).append("\n");
    stringBuilder.append(databaseExecutor == null ? "cache=null\n" : cache.toString()).append("\n");
    stringBuilder.append(databaseExecutor == null ? "hsClient=null\n" : databaseExecutor.toString());
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
  static abstract public class DistributedDataRepositoryRow extends DistributedElement implements AutoWritable,
      com.taobao.adfs.util.Cloneable {
    abstract public Object[] getKeys();

    abstract public String[] getStringKeys();

    abstract public String[] getStringValues();

    abstract public long getVersion();

    abstract public long setVersion(long newVersion);

    abstract public DistributedDataRepositoryRow update(DistributedDataRepositoryRow row, int fieldsIndication);

    abstract public DistributedDataRepositoryRow clone();

    @Override
    public String toString() {
      return Utilities.toStringByFields(this, true);
    }
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