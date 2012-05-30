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

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-11-1 下午4:10
 */
public class Get extends RequestWithCharest implements Request {
    private TableInfo tableInfo;
    private List<String[]> _key = new ArrayList<String[]>();
    private int ____find_flag = TDHSCommon.FindFlag.TDHS_EQ.getValue();
    private int _start;
    private int _limit;
    private Filters filters = new Filters();


    public Get(TableInfo tableInfo, String keys[][], TDHSCommon.FindFlag findFlag,
               int start,
               int limit) {
        this.tableInfo = tableInfo;
        if (keys != null) {
            Collections.addAll(this._key, keys);
        }
        this.____find_flag = findFlag.getValue();
        this._start = start;
        this._limit = limit;
    }

    public Get(TableInfo tableInfo, String keys[][], TDHSCommon.FindFlag findFlag,
               int start,
               int limit, @Nullable Filter filters[]) {
        this(tableInfo, keys, findFlag, start, limit);
        if (filters != null && filters.length > 0) {
            for (Filter f : filters) {
                this.addFilter(f);
            }
        }
    }

    public Get(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public List<String[]> getKey() {
        return _key;
    }

    public void setKey(String[] key) {
        _key.clear();
        _key.add(key);
    }

    public void setKey(String[][] keys) {
        _key.clear();
        if (keys != null) {
            Collections.addAll(this._key, keys);
        }
    }

    public void setKey(List<String>[] keys) {
        _key.clear();
        for (List<String> k : keys) {
            _key.add(k.toArray(new String[k.size()]));
        }
    }


    public int getFindFlag() {
        return ____find_flag;
    }

    public void setFindFlag(TDHSCommon.FindFlag findFlag) {
        this.____find_flag = findFlag.getValue();
    }

    public int getStart() {
        return _start;
    }

    public void setStart(int start) {
        this._start = start;
    }

    public int getLimit() {
        return _limit;
    }

    public void setLimit(int limit) {
        this._limit = limit;
    }

    public void addFilter(Filter filter) {
        filters.addFilter(filter);
    }

    public void addFilter(String field, TDHSCommon.FilterFlag flag, String value) {
        filters.addFilter(field, flag, value);
    }

    public void isVaild() throws TDHSEncodeException {
        if (tableInfo == null) {
            throw new TDHSEncodeException("tableInfo can't be empty!");
        }
        tableInfo.isVaild();
        if (_key == null || _key.size() == 0) {
            throw new TDHSEncodeException("key can't be missing!");
        }

        if (_key.size() > TDHSCommon.REQUEST_MAX_KEY_NUM) {
            throw new TDHSEncodeException("too many keys(in) ,larger than 10!");
        }

        for (String[] k : _key) {
            if (k == null || k.length == 0) {
                throw new TDHSEncodeException("key can't be empty!");
            }
            if (k.length > TDHSCommon.REQUEST_MAX_KEY_NUM) {
                throw new TDHSEncodeException("too many keys ,larger than 10!");
            }
        }
        if (filters != null) {
            filters.isVaild();
        }

    }


    @Override public String toString() {
        return "Get{" +
                "tableInfo=" + tableInfo +
                ",key=" + _key +
                ", find_flag=" + ____find_flag +
                ", start=" + _start +
                ", limit=" + _limit +
                ", filters=" + filters +
                '}';
    }
}
