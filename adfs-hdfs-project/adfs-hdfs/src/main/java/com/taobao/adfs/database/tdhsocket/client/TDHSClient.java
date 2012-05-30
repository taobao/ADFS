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

package com.taobao.adfs.database.tdhsocket.client;

import com.taobao.adfs.database.tdhsocket.client.statement.BatchStatement;
import com.taobao.adfs.database.tdhsocket.client.statement.Statement;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-26 下午4:53
 */
public interface TDHSClient extends Statement {

    public static final String CONNECTION_NUMBER = "connectionNumber";
    public static final String TIME_OUT = "timeOut";
    public static final String NEED_RECONNECT = "needReconnect";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String CHAREST_NAME = "charestName";
    public static final String READ_CODE = "readCode";
    public static final String WRITE_CODE = "writeCode";


    String getCharestName();

    void setCharestName(String charestName);

    Statement createStatement();
    
    Statement createStatement(int hash);

    BatchStatement createBatchStatement();

    void shutdown();
}
