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

package com.taobao.adfs.database.tdhsocket.client.easy.impl;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.easy.Set;
import com.taobao.adfs.database.tdhsocket.client.request.Get;
import com.taobao.adfs.database.tdhsocket.client.request.Update;

import org.jetbrains.annotations.Nullable;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午4:12
 */
public class SetImpl implements Set {

    private String field = null;

    private final Get get;

    private final Update update;

    private final Query query;

    public SetImpl(Get get, Update update, Query query) {
        this.get = get;
        this.query = query;
        this.update = update;
    }

    public Set field(String field) {
        if (this.field != null) {
            throw new IllegalArgumentException("can't field twice!");
        }
        this.field = field;
        return this;
    }

    private Query setIt(String value, TDHSCommon.UpdateFlag updateFlag) {
        if (this.field == null) {
            throw new IllegalArgumentException("no field!");
        }
        this.get.getTableInfo().getFields().add(this.field);
        this.field = null;
        this.update.addEntry(updateFlag, value);
        return query;
    }

    public Query add(long value) {
        return setIt(String.valueOf(value), TDHSCommon.UpdateFlag.TDHS_UPDATE_ADD);
    }

    public Query sub(long value) {
        return setIt(String.valueOf(value), TDHSCommon.UpdateFlag.TDHS_UPDATE_SUB);
    }

    public Query set(@Nullable String value) {
        return setIt(value, TDHSCommon.UpdateFlag.TDHS_UPDATE_SET);
    }

    public Query set(long value) {
        return set(String.valueOf(value));
    }

    public Query set(int value) {
        return set(String.valueOf(value));
    }

    public Query set(short value) {
        return set(String.valueOf(value));
    }

    public Query set(char value) {
        return set(String.valueOf(value));
    }

    public Query setNow() {
        return setIt("", TDHSCommon.UpdateFlag.TDHS_UPDATE_NOW);
    }

    public Query setNull() {
        return set(null);
    }
}
