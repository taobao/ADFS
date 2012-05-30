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

package com.taobao.adfs.database.tdhsocket.client.request;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSEncodeException;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-13 上午11:36
 */
public class TableInfo implements Request {

    private String _db;
    private String _table;
    private String _index;
    private List<String> _fields = new ArrayList<String>(10);

    private boolean needField;

    public TableInfo() {
    }

    public TableInfo(String db, String table, String index, String[] fields) {
        this._db = db;
        this._table = table;
        this._index = index;
        if (fields != null) {
            Collections.addAll(this._fields, fields);
        }
        this.needField = true;
    }

    public String getDb() {
        return _db;
    }

    public void setDb(String db) {
        this._db = db;
    }

    public String getTable() {
        return _table;
    }

    public void setTable(String _table) {
        this._table = _table;
    }

    public String getIndex() {
        return _index;
    }

    public void setIndex(String _index) {
        this._index = _index;
    }

    public List<String> getFields() {
        return _fields;
    }

    public void setNeedField(boolean needField) {
        this.needField = needField;
    }


    public void isVaild() throws TDHSEncodeException {
        if (StringUtils.isBlank(_db)) {
            throw new TDHSEncodeException("db can't be empty!");
        }
        if (StringUtils.isBlank(_table)) {
            throw new TDHSEncodeException("table can't be empty!");
        }
        if (needField && (_fields == null || _fields.size() == 0)) {
            throw new TDHSEncodeException("field can't be empty!");
        }
        if (needField && _fields != null && _fields.size() > TDHSCommon.REQUEST_MAX_FIELD_NUM) {
            throw new TDHSEncodeException("too many field , larger than 256!");
        }

    }

    @Override public String toString() {
        return "TableInfo{" +
                "db='" + _db + '\'' +
                ", table='" + _table + '\'' +
                ", index='" + _index + '\'' +
                ", fields=" + _fields +
                ", needField=" + needField +
                '}';
    }
}
