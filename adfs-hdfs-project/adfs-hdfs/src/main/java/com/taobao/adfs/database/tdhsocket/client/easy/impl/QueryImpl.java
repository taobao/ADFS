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

import com.taobao.adfs.database.tdhsocket.client.easy.And;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.easy.Set;
import com.taobao.adfs.database.tdhsocket.client.easy.Where;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.request.Get;
import com.taobao.adfs.database.tdhsocket.client.request.TableInfo;
import com.taobao.adfs.database.tdhsocket.client.request.Update;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.statement.Statement;

import java.util.Collections;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午3:27
 */
public class QueryImpl implements Query {
    private final Get get;

    private final Update update;

    private final Where where;

    private final And and;

    private final Set set;

    private final Statement statement;

    public QueryImpl(Statement statement) {
        this.statement = statement;
        this.get = new Get(new TableInfo());
        this.update = new Update(this.get);
        this.where = new WhereImpl(get, this);
        this.and = new AndImpl(get, this);
        this.set = new SetImpl(get, update, this);
    }

    public Query use(String db) {
        get.getTableInfo().setDb(db);
        return this;
    }

    public Query from(String table) {
        get.getTableInfo().setTable(table);
        return this;
    }

    public Query select(String... fields) {
        if (fields != null) {
            Collections.addAll(get.getTableInfo().getFields(), fields);
        }
        return this;
    }

    public Where where() {
        return this.where;
    }

    public And and() {
        return this.and;
    }

    public Set set() {
        return this.set;
    }

    public Query limit(int start, int limit) {
        get.setStart(start);
        get.setLimit(limit);
        return this;
    }

    public TDHSResponse get() throws TDHSException {
        return statement.get(get);
    }

    public TDHSResponse delete() throws TDHSException {
        return statement.delete(get);
    }

    public TDHSResponse update() throws TDHSException {
        return statement.update(update);
    }

    public TDHSResponse get(String charestName) throws TDHSException {
        get.setCharestName(charestName);
        return statement.get(get);
    }

    public TDHSResponse delete(String charestName) throws TDHSException {
        get.setCharestName(charestName);
        return statement.delete(get);
    }

    public TDHSResponse update(String charestName) throws TDHSException {
        update.setCharestName(charestName);
        return statement.update(update);
    }
}
