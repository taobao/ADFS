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

package com.taobao.adfs.database.tdhsocket.client.easy;

import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-27 下午2:45
 */
public interface Query {

    Query use(String db);

    Query from(String table);

    Query select(String... fields);

    Where where();

    And and();

    Set set();

    Query limit(int start, int limit);

    TDHSResponse get() throws TDHSException;

    TDHSResponse delete() throws TDHSException;

    TDHSResponse update() throws TDHSException;

    TDHSResponse get(String charestName) throws TDHSException;

    TDHSResponse delete(String charestName) throws TDHSException;

    TDHSResponse update(String charestName) throws TDHSException;
}
