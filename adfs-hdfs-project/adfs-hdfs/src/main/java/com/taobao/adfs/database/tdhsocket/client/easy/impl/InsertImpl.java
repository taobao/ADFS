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

package com.taobao.adfs.database.tdhsocket.client.easy.impl;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.easy.Insert;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.request.TableInfo;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.statement.Statement;

import org.jetbrains.annotations.Nullable;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午4:21
 */
public class InsertImpl implements Insert {

    private final Statement statement;

    private final com.taobao.adfs.database.tdhsocket.client.request.Insert insert;

    public InsertImpl(Statement statement) {
        this.statement = statement;
        this.insert = new com.taobao.adfs.database.tdhsocket.client.request.Insert(new TableInfo());
    }

    public Insert use(String db) {
        insert.getTableInfo().setDb(db);
        return this;
    }

    public Insert from(String table) {
        insert.getTableInfo().setTable(table);
        return this;
    }

    public Insert value(String field, @Nullable String value) {
        insert.getTableInfo().getFields().add(field);
        insert.addValue(value);
        return this;
    }

    public Insert value(String field, long value) {
        return value(field, String.valueOf(value));
    }

    public Insert value(String field, int value) {
        return value(field, String.valueOf(value));
    }

    public Insert value(String field, short value) {
        return value(field, String.valueOf(value));
    }

    public Insert value(String field, char value) {
        return value(field, String.valueOf(value));
    }

    public Insert valueSetNow(String field) {
        insert.getTableInfo().getFields().add(field);
        insert.addValue(TDHSCommon.UpdateFlag.TDHS_UPDATE_NOW, "");
        return this;
    }

    public Insert valueSetNull(String field) {
        return value(field, null);
    }

    public Insert value(String field, TDHSCommon.UpdateFlag flag, String value) {
        insert.getTableInfo().getFields().add(field);
        insert.addValue(flag, value);
        return this;
    }

    public TDHSResponse insert() throws TDHSException {
        return statement.insert(insert);
    }

    public TDHSResponse insert(String charsetName) throws TDHSException {
        insert.setCharestName(charsetName);
        return statement.insert(insert);
    }

}
