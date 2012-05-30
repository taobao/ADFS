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

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-13 下午1:25
 */

public class Filter implements Request {
    private String _field;

    private int _____flag;

    private String _value;


    public Filter(String _field, TDHSCommon.FilterFlag flag, String value) {
        this._field = _field;
        this._____flag = flag.getValue();
        this._value = value;
    }

    public void isVaild() throws TDHSEncodeException {
        //nothing
    }

    @Override public String toString() {
        return "Filter{" +
                "field='" + _field + '\'' +
                ", flag=" + _____flag +
                ", value='" + _value + '\'' +
                '}';
    }
}