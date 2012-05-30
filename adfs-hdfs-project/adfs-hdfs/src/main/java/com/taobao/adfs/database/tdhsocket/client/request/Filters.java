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

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-13 下午1:01
 */
public class Filters implements Request {

    private List<Filter> _filter = new ArrayList<Filter>(1);

    public void addFilter(String field, TDHSCommon.FilterFlag flag, String value) {
        _filter.add(new Filter(field, flag, value));
    }

    public void addFilter(Filter filter) {
        _filter.add(filter);
    }


    public void isVaild() throws TDHSEncodeException {
        if (_filter != null && _filter.size() > TDHSCommon.REQUEST_MAX_FIELD_NUM) {
            throw new TDHSEncodeException("too many filter , larger than 256!");
        }
    }

    @Override public String toString() {
        return "Filters{" +
                "filter=" + _filter +
                '}';
    }
}
