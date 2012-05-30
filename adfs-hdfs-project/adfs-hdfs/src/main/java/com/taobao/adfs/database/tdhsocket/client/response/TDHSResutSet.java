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

package com.taobao.adfs.database.tdhsocket.client.response;

import com.taobao.adfs.database.tdhsocket.client.util.ConvertUtil;

import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-1-13 下午1:23
 */
public class TDHSResutSet implements ResultSet {

    private Map<String, Integer> fieldMap = new HashMap<String, Integer>();

    private List<TDHSResponseEnum.FieldType> fieldTypes = new ArrayList<TDHSResponseEnum.FieldType>();

    private List<List<String>> fieldData;

    private ListIterator<List<String>> iterator;

    private List<String> currentRow;

    private int index = -1;

    private boolean lastWasNull = false;

    public TDHSResutSet(List<String> fieldNames, List<TDHSResponseEnum.FieldType> fieldTypes,
                        List<List<String>> fieldData) {
        if (fieldNames != null) {
            int i = 1;
            for (String f : fieldNames) {
                fieldMap.put(f, i++);
            }
        }
        if (fieldTypes != null) {
            this.fieldTypes = fieldTypes;
        }

        this.fieldData = fieldData;
        if (fieldData != null) {
            this.iterator = fieldData.listIterator();
        }

    }

    public boolean next() throws SQLException {
        if (iterator != null && iterator.hasNext()) {
            currentRow = iterator.next();
            index++;
            return true;
        }
        return false;
    }

    public void close() throws SQLException {
        //do nothing
    }

    public boolean wasNull() throws SQLException {
        return this.lastWasNull;
    }

    private void checkRow(int columnIndex) throws SQLException {
        if (currentRow == null) {
            throw new SQLException("can't find current row");
        }
        if (columnIndex <= 0 || columnIndex > currentRow.size()) {
            throw new SQLException("Invaild column:" + columnIndex);
        }
    }

    private void checkType(int columnIndex) throws SQLException {
        if (columnIndex <= 0 || columnIndex > fieldTypes.size()) {
            throw new SQLException("Invaild column:" + columnIndex);
        }
    }

    public String getString(int columnIndex) throws SQLException {
        this.checkRow(columnIndex);
        String str = currentRow.get(columnIndex - 1);
        this.lastWasNull = (str == null);
        return StringUtils.trim(str);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return ConvertUtil.getBooleanFromString(this.getString(columnIndex));
    }

    public byte getByte(int columnIndex) throws SQLException {
        return ConvertUtil.getByteFromString(this.getString(columnIndex));
    }


    public short getShort(int columnIndex) throws SQLException {
        return ConvertUtil.getShortFromString(this.getString(columnIndex));
    }

    public int getInt(int columnIndex) throws SQLException {
        return ConvertUtil.getIntFromString(this.getString(columnIndex));
    }

    public long getLong(int columnIndex) throws SQLException {
        return ConvertUtil.getLongFromString(this.getString(columnIndex));
    }

    public float getFloat(int columnIndex) throws SQLException {
        return ConvertUtil.getFloatFromString(this.getString(columnIndex));
    }

    public double getDouble(int columnIndex) throws SQLException {
        return ConvertUtil.getDoubleFromString(this.getString(columnIndex));
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return ConvertUtil.getBigDecimalFromString(this.getString(columnIndex), scale);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        String s = this.getString(columnIndex);
        return s == null ? null : s.getBytes();
    }

    public Date getDate(int columnIndex) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), null);
        return time == null ? null : new Date(time);
    }

    public Time getTime(int columnIndex) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), null);
        return time == null ? null : new Time(time);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), null);
        return time == null ? null : new Timestamp(time);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        byte[] b = this.getBytes(columnIndex);
        return b == null ? null : new ByteArrayInputStream(b);
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return this.getAsciiStream(columnIndex);
    }

    public String getString(String columnLabel) throws SQLException {
        return this.getString(this.findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.getBoolean(this.findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return this.getByte(this.findColumn(columnLabel));
    }

    public short getShort(String columnLabel) throws SQLException {
        return this.getShort(this.findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel), scale);
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.getBytes(this.findColumn(columnLabel));
    }

    public Date getDate(String columnLabel) throws SQLException {
        return this.getDate(this.findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        return this.getTime(this.findColumn(columnLabel));
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.getTimestamp(this.findColumn(columnLabel));
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return this.getAsciiStream(this.findColumn(columnLabel));
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return this.getUnicodeStream(this.findColumn(columnLabel));
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return this.getBinaryStream(this.findColumn(columnLabel));
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(int columnIndex) throws SQLException {
        this.checkRow(columnIndex);
        this.checkType(columnIndex);
        TDHSResponseEnum.FieldType fieldType = fieldTypes.get(columnIndex);
        switch (fieldType) {
            case MYSQL_TYPE_DECIMAL:
                return this.getBigDecimal(columnIndex);
            case MYSQL_TYPE_TINY:
                return this.getByte(columnIndex);
            case MYSQL_TYPE_SHORT:
                return this.getShort(columnIndex);
            case MYSQL_TYPE_LONG:
                return this.getLong(columnIndex);
            case MYSQL_TYPE_FLOAT:
                return this.getFloat(columnIndex);
            case MYSQL_TYPE_DOUBLE:
                return this.getDouble(columnIndex);
            case MYSQL_TYPE_NULL:
                return null;
            case MYSQL_TYPE_TIMESTAMP:
                return this.getTimestamp(columnIndex);
            case MYSQL_TYPE_LONGLONG:
                return this.getBigDecimal(columnIndex);
            case MYSQL_TYPE_INT24:
                return this.getBigDecimal(columnIndex);
            case MYSQL_TYPE_DATE:
                return this.getDate(columnIndex);
            case MYSQL_TYPE_TIME:
                return this.getTime(columnIndex);
            case MYSQL_TYPE_DATETIME:
                return this.getDate(columnIndex);
            case MYSQL_TYPE_YEAR:
                return this.getDate(columnIndex);
            case MYSQL_TYPE_NEWDATE:
                return this.getDate(columnIndex);
            case MYSQL_TYPE_VARCHAR:
                return this.getString(columnIndex);
            case MYSQL_TYPE_BIT:
                return this.getBoolean(columnIndex);
            case MYSQL_TYPE_NEWDECIMAL:
                return this.getBigDecimal(columnIndex);
            case MYSQL_TYPE_ENUM:
                return this.getString(columnIndex);
            case MYSQL_TYPE_SET:
                return this.getString(columnIndex);
            case MYSQL_TYPE_TINY_BLOB:
                return this.getBytes(columnIndex);
            case MYSQL_TYPE_MEDIUM_BLOB:
                return this.getBytes(columnIndex);
            case MYSQL_TYPE_LONG_BLOB:
                return this.getBytes(columnIndex);
            case MYSQL_TYPE_BLOB:
                return this.getBytes(columnIndex);
            case MYSQL_TYPE_VAR_STRING:
                return this.getString(columnIndex);
            case MYSQL_TYPE_STRING:
                return this.getString(columnIndex);
            case MYSQL_TYPE_GEOMETRY:
                return this.getString(columnIndex);
        }
        return null;
    }

    public Object getObject(String columnLabel) throws SQLException {
        return this.getObject(this.findColumn(columnLabel));
    }

    public int findColumn(String columnLabel) throws SQLException {
        if (!fieldMap.containsKey(columnLabel)) {
            throw new SQLException("columnLabel " + columnLabel
                    + " is not in result set");
        }
        return fieldMap.get(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String stringVal = this.getString(columnIndex);
        if (stringVal == null) {
            return null;
        }
        return new StringReader(stringVal);
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return this.getCharacterStream(this.findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return ConvertUtil.getBigDecimalFromString(this.getString(columnIndex));
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return this.getBigDecimal(this.findColumn(columnLabel));
    }

    public boolean isBeforeFirst() throws SQLException {
        return index < 0;
    }

    public boolean isAfterLast() throws SQLException {
        return fieldData != null && index >= fieldData.size();
    }

    public boolean isFirst() throws SQLException {
        return index == 0;
    }

    public boolean isLast() throws SQLException {
        return fieldData != null && index == fieldData.size() - 1;
    }

    public void beforeFirst() throws SQLException {
        currentRow = null;
        index = -1;
        if (fieldData != null) {
            this.iterator = fieldData.listIterator();
        }
    }

    public void afterLast() throws SQLException {
        currentRow = null;
        index = fieldData != null ? fieldData.size() : -1;
        this.iterator = null;
    }

    public boolean first() throws SQLException {
        beforeFirst();
        return next();
    }

    public boolean last() throws SQLException {
        if (fieldData != null && !fieldData.isEmpty()) {
            index = fieldData.size() - 1;
            currentRow = fieldData.get(index);
            iterator = null; //it is last,didn't need  iterator
            return true;

        }
        return false;
    }

    public int getRow() throws SQLException {
        return index + 1;
    }

    public boolean absolute(int row) throws SQLException {
        if (fieldData == null || row <= 0 || row > fieldData.size()) {
            throw new SQLException("Invaild row:" + row);
        }
        if (fieldData != null && !fieldData.isEmpty()) {
            index = row - 1;
            currentRow = fieldData.get(index);
            iterator = fieldData.listIterator(index);
            return true;

        }
        return false;
    }

    public boolean relative(int rows) throws SQLException {
        return absolute(index + rows);
    }

    public boolean previous() throws SQLException {
        if (iterator != null && iterator.hasPrevious()) {
            currentRow = iterator.previous();
            index--;
            return true;
        }
        return false;
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), cal);
        return time == null ? null : new Date(time);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(this.findColumn(columnLabel)), cal);
        return time == null ? null : new Date(time);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), cal);
        return time == null ? null : new Time(time);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(this.findColumn(columnLabel)), cal);
        return time == null ? null : new Time(time);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(columnIndex), cal);
        return time == null ? null : new Timestamp(time);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        Long time = ConvertUtil.getTimeFromString(this.getString(this.findColumn(columnLabel)), cal);
        return time == null ? null : new Timestamp(time);
    }

    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(this.getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    public URL getURL(String columnLabel) throws SQLException {
        return this.getURL(this.findColumn(columnLabel));
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isClosed() throws SQLException {
        return true;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;  //FIXME 编写实现
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;  //FIXME 编写实现
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping
            // anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException(cce);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
