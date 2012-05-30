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

package com.taobao.adfs.database.tdhsocket.client.request;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSEncodeException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-14 下午4:28
 */
public class Update extends RequestWithCharest implements Request {

    private Get get;

    private List<ValueEntry> _valueEntries = new ArrayList<ValueEntry>(1);

    public Update(Get get) {
        this.get = get;
    }

    public Update(Get get, ValueEntry valueEntry[]) {
        this(get);
        if (valueEntry != null && valueEntry.length > 0) {
            for (ValueEntry u : valueEntry) {
                this.addEntry(u);
            }
        }
    }


    public void addEntry(TDHSCommon.UpdateFlag flag, String value) {
        _valueEntries.add(new ValueEntry(flag, value));
    }

    public void addEntry(ValueEntry entry) {
        _valueEntries.add(entry);
    }


    public void isVaild() throws TDHSEncodeException {
        if (get == null) {
            throw new TDHSEncodeException("get can't be empty!");
        }
        get.isVaild();
        if (_valueEntries.size() != get.getTableInfo().getFields().size()) {
            throw new TDHSEncodeException("field's size not match updateEntries's size");
        }
        if (_valueEntries.size() > TDHSCommon.REQUEST_MAX_FIELD_NUM) {
            throw new TDHSEncodeException("too many updateEntries , larger than 256!");
        }
    }

    @Override public String toString() {
        return "Update{" +
                "get=" + get +
                ", valueEntries=" + _valueEntries +
                '}';
    }
}
