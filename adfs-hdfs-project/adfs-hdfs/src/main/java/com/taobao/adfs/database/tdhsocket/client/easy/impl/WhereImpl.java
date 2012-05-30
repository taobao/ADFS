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

package com.taobao.adfs.database.tdhsocket.client.easy.impl;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.easy.Where;
import com.taobao.adfs.database.tdhsocket.client.request.Get;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午3:48
 */
public class WhereImpl implements Where {

    private final Get get;

    private final Query query;

    public WhereImpl(Get get, Query query) {
        this.get = get;
        this.query = query;
    }

    public Where fields(String... field) {
        StringBuilder sb = new StringBuilder("|");
        for (String f : field) {
            if (StringUtils.isNotBlank((f))) {
                sb.append(f);
                sb.append('|');
            }
        }
        get.getTableInfo().setIndex(sb.toString());
        return this;
    }

    public Where index(String index) {
        get.getTableInfo().setIndex(index);
        return this;
    }

    public Query equal(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_EQ);
        get.setKey(key);
        return query;
    }

    public Query descEqual(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_DEQ);
        get.setKey(key);
        return query;
    }

    public Query greaterEqual(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_GE);
        get.setKey(key);
        return query;
    }

    public Query lessEqual(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_LE);
        get.setKey(key);
        return query;
    }

    public Query greaterThan(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_GT);
        get.setKey(key);
        return query;
    }

    public Query lessThan(String... key) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_LT);
        get.setKey(key);
        return query;
    }

    public Query in(String[]... keys) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_IN);
        get.setKey(keys);
        return query;
    }

    public Query in(List<String>... keys) {
        get.setFindFlag(TDHSCommon.FindFlag.TDHS_IN);
        get.setKey(keys);
        return query;
    }
}
