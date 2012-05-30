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
import com.taobao.adfs.database.tdhsocket.client.easy.And;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.request.Get;

import org.jetbrains.annotations.Nullable;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午3:55
 */
public class AndImpl implements And {

    private String field = null;

    private final Get get;

    private final Query query;

    public AndImpl(Get get, Query query) {
        this.query = query;
        this.get = get;
    }

    public And field(String field) {
        if (this.field != null) {
            throw new IllegalArgumentException("can't field twice!");
        }
        this.field = field;
        return this;
    }

    private Query filter(@Nullable String value, TDHSCommon.FilterFlag filterFlag) {
        if (this.field == null) {
            throw new IllegalArgumentException("no field!");
        }
        this.get.addFilter(field, filterFlag, value);
        this.field = null;
        return query;
    }

    public Query equal(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_EQ);
    }


    public Query greaterEqual(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_GE);
    }

    public Query lessEqual(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_LE);
    }

    public Query greaterThan(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_GT);
    }

    public Query lessThan(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_LT);
    }

    public Query not(String value) {
        return filter(value, TDHSCommon.FilterFlag.TDHS_NOT);
    }

    public Query isNull() {
        return filter(null, TDHSCommon.FilterFlag.TDHS_EQ);
    }
}
