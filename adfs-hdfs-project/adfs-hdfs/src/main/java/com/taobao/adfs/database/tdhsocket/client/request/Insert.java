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

package com.taobao.adfs.database.tdhsocket.client.request;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSEncodeException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-19 下午5:45
 */
public class Insert extends RequestWithCharest implements Request {
    private TableInfo tableInfo;

    private List<ValueEntry> _values = new ArrayList<ValueEntry>();

    public Insert(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public Insert(TableInfo tableInfo, ValueEntry values[]) {
        this(tableInfo);
        if (values != null && values.length > 0) {
            for (ValueEntry u : values) {
                this.addValue(u);
            }
        }
    }

    public Insert(TableInfo tableInfo, String values[]) {
        this(tableInfo);
        if (values != null && values.length > 0) {
            for (String u : values) {
                this.addValue(u);
            }
        }
    }

    public void addValue(String entry) {
        addValue(TDHSCommon.UpdateFlag.TDHS_UPDATE_SET, entry);
    }

    public void addValue(TDHSCommon.UpdateFlag flag, String value) {
        _values.add(new ValueEntry(flag, value));
    }

    public void addValue(ValueEntry entry) {
        _values.add(entry);
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void isVaild() throws TDHSEncodeException {
        if (tableInfo == null) {
            throw new TDHSEncodeException("tableInfo can't be empty!");
        }
        tableInfo.isVaild();
        if (_values.size() != tableInfo.getFields().size()) {
            throw new TDHSEncodeException("field's size not match values's size");
        }
        if (_values.size() > TDHSCommon.REQUEST_MAX_FIELD_NUM) {
            throw new TDHSEncodeException("too many insert values , larger than 256!");
        }
    }

    @Override public String toString() {
        return "Insert{" +
                "tableInfo=" + tableInfo +
                ", values=" + _values +
                '}';
    }
}
