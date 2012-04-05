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

package com.taobao.adfs.database.handlersocket;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.IndexSession;
import com.google.code.hs4j.ModifyStatement;
import com.google.code.hs4j.exception.HandlerSocketException;
import com.google.code.hs4j.impl.IndexSessionImpl;
import com.google.code.hs4j.impl.ResultSetImpl;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-06-17
 */
public class HSClientSimulator implements HSClient {
  private String encoding = "utf-8";
  Map<String, Table> tables = new HashMap<String, Table>();
  Map<Integer, OpenedIndex> openedIndexes = new HashMap<Integer, OpenedIndex>();
  Configuration conf = null;
  /**
   * Index id counter
   */
  private static AtomicInteger INDEX_COUNTER = new AtomicInteger();

  public HSClientSimulator(Configuration conf) throws IOException {
    this.conf = conf;
    describeTable();
  }

  private void describeTable() throws IOException {
    String databaseDescription = conf.get("database.executor.handlersocket.simulator.description");
    databaseDescription.replaceAll(";;", ";");
    if (databaseDescription.startsWith(";")) databaseDescription = databaseDescription.substring(1);

    // parse database description
    // tableDescriptions: tableDescription1;tableDescription2
    // tableDescription : dbName.tableName:indexDescription1|indexDescription2
    // indexDescription : indexName=fieldIndexNum1,fieldIndexNum2
    // example: jiwan.table.test:id=integer|name=string:PRIMARY=0|ID_NAME=0,1;
    String[] tableDescriptions = databaseDescription.split(";");
    for (String tableDescription : tableDescriptions) {
      String[] tableMetaItems = tableDescription.split(":");
      String tableName = tableMetaItems[0];
      String[] fields = tableMetaItems[1].split("\\|");
      String[] fieldNames = new String[fields.length];
      String[] fieldTypes = new String[fields.length];
      for (int i = 0; i < fields.length; ++i) {
        String[] fieldParts = fields[i].split("=");
        if (fieldParts.length != 2)
          throw new IOException("fail to parse " + fields[i] + "@" + databaseDescription
              + " Make sure format is FieldName=FieldType, FieldType can be integer or string");
        fieldNames[i] = fieldParts[0];
        fieldTypes[i] = fieldParts[1];
      }
      Map<String, int[]> indexes = new HashMap<String, int[]>();
      for (String indexDescription : tableMetaItems[2].split("\\|")) {
        String[] indexMetaItems = indexDescription.split("=");
        String indexName = indexMetaItems[0];
        String[] indexFieldNumberStrings = indexMetaItems[1].split(",");
        int[] indexFieldNumbers = new int[indexFieldNumberStrings.length];
        for (int i = 0; i < indexFieldNumberStrings.length; ++i) {
          indexFieldNumbers[i] = Integer.parseInt(indexFieldNumberStrings[i]);
        }
        indexes.put(indexName, indexFieldNumbers);
      }
      Table table = new Table(tableName, fieldNames, fieldTypes, indexes);
      tables.put(tableName, table);
    }
  }

  @Override
  public synchronized boolean insert(int indexId, String[] values) throws InterruptedException, TimeoutException,
      HandlerSocketException {
    throwException("hsclient.simulator.throw.exception.for.insert");

    OpenedIndex openedIndex = openedIndexes.get(indexId);
    String[] row = new String[openedIndex.table.fieldNames.length];
    openedIndex.updateRow(row, values);
    String keyString = openedIndex.getKeyString(values);

    // check existence of this record
    if (openedIndex.getRows().get(keyString) != null) throw new HandlerSocketException("record is already existed");
    // insert this record
    openedIndex.getRows().put(keyString, values.clone());
    return true;
  }

  @Override
  public synchronized int delete(int indexId, String[] values) throws InterruptedException, TimeoutException,
      HandlerSocketException {
    return delete(indexId, values, FindOperator.EQ);
  }

  @Override
  public synchronized int update(int indexId, String[] keys, String[] values, FindOperator operator)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    return update(indexId, keys, values, operator, 1, 0);
  }

  @Override
  public ResultSet find(int indexId, String[] values) throws InterruptedException, TimeoutException,
      HandlerSocketException {
    return find(indexId, values, FindOperator.EQ, 1, 0);
  }

  @Override
  public boolean openIndex(int indexId, String dbname, String tableName, String indexName, String[] columns)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    OpenedIndex openedIndex = new OpenedIndex(tables.get(dbname + "." + tableName), indexName, columns);
    openedIndexes.put(indexId, openedIndex);
    return true;
  }

  @Override
  public ResultSet find(int indexId, String[] keys, FindOperator operator, int limit, int offset)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    throwException("hsclient.simulator.throw.exception.for.find");

    OpenedIndex openedIndex = openedIndexes.get(indexId);
    List<List<byte[]>> rowsForResultSet = new ArrayList<List<byte[]>>();
    for (String[] row : findInternal(openedIndex, keys, operator, limit, offset)) {
      row = openedIndex.getRow(row);
      List<byte[]> columns = new ArrayList<byte[]>(1);
      for (String column : row) {
        columns.add(column == null ? null : column.getBytes());
      }
      rowsForResultSet.add(columns);
    }

    return new ResultSetImpl(rowsForResultSet, openedIndex.table.fieldNames, encoding);
  }

  public List<String[]> findInternal(OpenedIndex openedIndex, String[] keys, FindOperator operator, int limit,
      int offset) throws InterruptedException, TimeoutException, HandlerSocketException {
    if (limit == 0) limit = 1;
    int[] keyIndexes = openedIndex.getIndex();
    Map<String, String[]> rows = openedIndex.getRows();
    List<String[]> matchedRows = new ArrayList<String[]>();
    for (String[] row : rows.values()) {
      // is this row matched?
      boolean matched = true;
      for (int i = 0; i < keys.length; ++i) {
        if (operator.equals(FindOperator.EQ)) {
          if ((row[keyIndexes[i]] == null && keys[i] != null)
              || (row[keyIndexes[i]] != null && openedIndex.table.compareValue(keyIndexes[i], row[keyIndexes[i]],
                  keys[i]) != 0)) {
            matched = false;
            break;
          }
        } else {
          if (keys.length != 1) throw new HandlerSocketException("only support 1 key for operator=" + operator);
          if (operator.equals(FindOperator.GT)) {
            if (row[keyIndexes[i]] == null
                || openedIndex.table.compareValue(keyIndexes[i], row[keyIndexes[i]], keys[0]) <= 0) {
              matched = false;
              break;
            }
          } else if (operator.equals(FindOperator.GE)) {
            if (row[keyIndexes[i]] == null
                || openedIndex.table.compareValue(keyIndexes[i], row[keyIndexes[i]], keys[0]) < 0) {
              matched = false;
              break;
            }
          } else if (operator.equals(FindOperator.LT)) {
            if (row[keyIndexes[i]] == null
                || openedIndex.table.compareValue(keyIndexes[i], row[keyIndexes[i]], keys[0]) >= 0) {
              matched = false;
              break;
            }
          } else if (operator.equals(FindOperator.LE)) {
            if (row[keyIndexes[i]] == null
                || openedIndex.table.compareValue(keyIndexes[i], row[keyIndexes[i]], keys[0]) > 0) {
              matched = false;
              break;
            }
          }
        }
      }
      // add into result if row is matched
      if (matched) {
        if (--limit < 0) break;
        matchedRows.add(row.clone());
      }
    }

    return matchedRows;
  }

  void throwException(String keyName) throws InterruptedException, TimeoutException, HandlerSocketException {
    String throwException = conf.get(keyName, "");
    if (throwException.equals(InterruptedException.class.getName())) throw new InterruptedException();
    if (throwException.equals(TimeoutException.class.getName())) throw new TimeoutException();
    if (throwException.equals(HandlerSocketException.class.getName())) throw new HandlerSocketException();
  }

  @Override
  public int update(int indexId, String[] keys, String[] values, FindOperator operator, int limit, int offset)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    throwException("hsclient.simulator.throw.exception.for.update");

    OpenedIndex openedIndex = openedIndexes.get(indexId);
    List<String[]> rows = findInternal(openedIndex, keys, operator, limit, offset);
    boolean foundRow = false;
    for (String[] row : rows) {
      foundRow = true;
      String keyString = openedIndex.getKeyString(row);
      openedIndex.updateRow(row, values);
      String newKeyOfRowInRows = openedIndex.getKeyString(row);
      if (newKeyOfRowInRows.equals(keyString)) {
        openedIndex.getRows().put(keyString, row);
      } else {
        if (!newKeyOfRowInRows.equals(keyString) && openedIndex.getRows().containsKey(newKeyOfRowInRows))
          throw new HandlerSocketException("record is existed");
        openedIndex.getRows().remove(keyString);
        openedIndex.getRows().put(newKeyOfRowInRows, row);
      }
    }
    return foundRow ? 1 : 0;
  }

  @Override
  public int delete(int indexId, String[] keys, FindOperator operator, int limit, int offset)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    throwException("hsclient.simulator.throw.exception.for.delete");

    OpenedIndex openedIndex = openedIndexes.get(indexId);
    List<String[]> rows = findInternal(openedIndex, keys, operator, limit, offset);
    boolean foundRow = false;
    for (String[] row : rows) {
      foundRow = true;
      openedIndex.getRows().remove(openedIndex.getKeyString(row));
    }
    return foundRow ? 1 : 0;
  }

  @Override
  public int delete(int indexId, String[] values, FindOperator operator) throws InterruptedException, TimeoutException,
      HandlerSocketException {
    return delete(indexId, values, operator, 1, 0);
  }

  @Override
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  @Override
  public String getEncoding() {
    return encoding;
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public void shutdown() throws IOException {
  }

  @Override
  public boolean isAllowAutoReconnect() {
    return true;
  }

  @Override
  public void setOpTimeout(long opTimeout) {
  }

  @Override
  public IndexSession openIndexSession(int indexId, String dbname, String tableName, String indexName, String[] columns)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    if (openIndex(indexId, dbname, tableName, indexName, columns)) {
      return new IndexSessionImpl(this, indexId, columns);
    } else {
      return null;
    }
  }

  @Override
  public IndexSession openIndexSession(String dbname, String tableName, String indexName, String[] columns)
      throws InterruptedException, TimeoutException, HandlerSocketException {
    return openIndexSession(INDEX_COUNTER.incrementAndGet(), dbname, tableName, indexName, columns);
  }

  @Override
  public void setHealConnectionInterval(long interval) {
  }

  @Override
  public long getHealConnectionInterval() {
    return 0;
  }

  @Override
  public void setAllowAutoReconnect(boolean allowAutoReconnect) {
  }

  @Override
  public ModifyStatement createStatement(int indexId) throws HandlerSocketException {
    return null;
  }

  public String toString() {
    StringBuilder databaseContent = new StringBuilder();
    for (String tableName : tables.keySet()) {
      databaseContent.append(tableName).append("={");
      for (String[] row : tables.get(tableName).rows.values()) {
        databaseContent.append('\n').append(Arrays.deepToString(row));
      }
      databaseContent.append("\n}\n");
    }
    return databaseContent.toString();
  }

  static class Table {
    String tableName = null;
    String[] fieldNames = null;
    String[] fieldTypes = null;
    Map<String, int[]> indexes = null;
    Map<String, String[]> rows = new ConcurrentHashMap<String, String[]>();

    Table(String tableName, String[] fieldValues, String[] fieldTypes, Map<String, int[]> indexes) {
      this.tableName = tableName;
      this.fieldNames = fieldValues.clone();
      this.fieldTypes = fieldTypes.clone();
      this.indexes = indexes;
    }

    int[] getPrimaryIndex() {
      return indexes.get("PRIMARY");
    }

    int compareValue(int columnIndex, String columnValue, String anotherValue) {
      if (fieldTypes[columnIndex].equals("integer")) return Long.valueOf(columnValue).compareTo(
          Long.valueOf(anotherValue));
      else return columnValue.compareTo(anotherValue);
    }
  }

  static class OpenedIndex {
    Table table = null;
    String indexName = null;
    String[] columns = null;

    OpenedIndex(Table table, String indexName, String[] columns) {
      this.table = table;
      this.indexName = indexName;
      this.columns = columns.clone();
    }

    int[] getIndex() {
      return table.indexes.get(indexName);
    }

    Map<String, String[]> getRows() {
      return table.rows;
    }

    // get interested row data according to columns
    String[] getRow(String[] rowInRows) throws HandlerSocketException {
      String[] row = new String[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        for (int j = 0; j < table.fieldNames.length; ++j) {
          if (columns[i].equals(table.fieldNames[j])) {
            row[i] = rowInRows[j];
            break;
          }
          if (j == table.fieldNames.length - 1)
            throw new HandlerSocketException("fail to get row for index:" + indexName);
        }
      }
      return row;
    }

    void updateRow(String[] rowInRows, String[] rowInIndex) throws HandlerSocketException {
      for (int i = 0; i < columns.length; ++i) {
        for (int j = 0; j < table.fieldNames.length; ++j) {
          if (columns[i].equals(table.fieldNames[j])) {
            rowInRows[j] = rowInIndex[i];
            break;
          }
          if (j == table.fieldNames.length - 1)
            throw new HandlerSocketException("fail to get row for index:" + indexName);
        }
      }
    }

    String getKeyString(String[] rowInRows) {
      String keyString = "";
      for (int columnIndex : table.getPrimaryIndex()) {
        keyString += table.fieldNames[columnIndex];
        keyString += "=";
        keyString += rowInRows[columnIndex];
        keyString += ",";
      }
      return keyString;
    }
  }
}
