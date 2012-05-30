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
 * @since 11-12-14 下午4:30
 */
public class ValueEntry implements Request {
    private int ____flag;

    private String _value;

    public ValueEntry(TDHSCommon.UpdateFlag flag, String _value) {
        this.____flag = flag.getValue();
        this._value = _value;
    }

    public void isVaild() throws TDHSEncodeException {
    }

    @Override public String toString() {
        return "ValueEntry{" +
                "flag=" + ____flag +
                ", value='" + _value + '\'' +
                '}';
    }
}
