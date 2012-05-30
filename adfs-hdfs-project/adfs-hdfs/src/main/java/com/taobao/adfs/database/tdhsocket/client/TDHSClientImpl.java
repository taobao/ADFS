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

package com.taobao.adfs.database.tdhsocket.client;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSTimeoutException;
import com.taobao.adfs.database.tdhsocket.client.net.NetParameters;
import com.taobao.adfs.database.tdhsocket.client.net.TDHSNet;
import com.taobao.adfs.database.tdhsocket.client.net.netty.TDHSNetForNetty;
import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;
import com.taobao.adfs.database.tdhsocket.client.protocol.TDHSProtocol;
import com.taobao.adfs.database.tdhsocket.client.request.*;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.statement.BatchStatement;
import com.taobao.adfs.database.tdhsocket.client.statement.BatchStatementImpl;
import com.taobao.adfs.database.tdhsocket.client.statement.Statement;
import com.taobao.adfs.database.tdhsocket.client.statement.StatementImpl;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-10-31 下午1:32
 */
public class TDHSClientImpl implements TDHSClient {

    private final TDHSNet tdhsNet;

    private static final AtomicLong id = new AtomicLong(1L);

    private static final ConcurrentHashMap<Long, ArrayBlockingQueue<BasePacket>> responses =
            new ConcurrentHashMap<Long, ArrayBlockingQueue<BasePacket>>();

    private final TDHSProtocol protocol;

    private final int timeOut; //ms

    private String charestName;


    public TDHSClientImpl(InetSocketAddress address, Map props) throws TDHSException {
        this(address, props.containsKey(CONNECTION_NUMBER) ? (Integer) props.get(CONNECTION_NUMBER) : 1,
                props.containsKey(TIME_OUT) ? (Integer) props.get(TIME_OUT) : 1000,
                props.containsKey(NEED_RECONNECT) ? (Boolean) props.get(NEED_RECONNECT) : true,
                props.containsKey(CONNECT_TIMEOUT) ? (Integer) props.get(CONNECT_TIMEOUT) : 1000,
                props.containsKey(CHAREST_NAME) ? (String) props.get(CHAREST_NAME) : null,
                props.containsKey(READ_CODE) ? (String) props.get(READ_CODE) : null,
                props.containsKey(WRITE_CODE) ? (String) props.get(WRITE_CODE) : null);
    }

    public TDHSClientImpl(InetSocketAddress address, int connectionNumber) throws TDHSException {
        this(address, connectionNumber, 1000);
    }

    public TDHSClientImpl(InetSocketAddress address, int connectionNumber, int timeOut) throws TDHSException {
        this(address, connectionNumber, timeOut, true, 1000);
    }

    public TDHSClientImpl(InetSocketAddress address, int connectionNumber, int timeOut, boolean needReconnect,
                          int connectTimeOut) throws TDHSException {
        this(address, connectionNumber, timeOut, needReconnect, connectTimeOut, null, null, null);
    }

    public TDHSClientImpl(InetSocketAddress address, int connectionNumber, int timeOut, boolean needReconnect,
                          int connectTimeOut, @Nullable String charestName, @Nullable String readCode,
                          @Nullable String writeCode)
            throws TDHSException {

        if (connectionNumber <= 0) {
            throw new IllegalArgumentException("connectionNumber must be positive!");
        }
        this.timeOut = timeOut;
        protocol = TDHSCommon.PROTOCOL_FOR_BINARY;
        tdhsNet = new TDHSNetForNetty();
        this.charestName = charestName;
        NetParameters parameters = new NetParameters();
        parameters.setAddress(address);
        parameters.setConnectionNumber(connectionNumber);
        parameters.setNeedReconnect(needReconnect);
        tdhsNet.initNet(parameters, protocol.shakeHandPacket(this.timeOut, readCode, writeCode), responses);
        if (!awaitForConnected(connectTimeOut, TimeUnit.MILLISECONDS)) {
            throw new TDHSTimeoutException("connect time out");
        }
    }


    private boolean awaitForConnected(long timeout, TimeUnit unit) {
        return tdhsNet.awaitForConnected(timeout, unit);
    }

    public String getCharestName() {
        return charestName;
    }

    public void setCharestName(String charestName) {
        this.charestName = charestName;
    }

    public Statement createStatement() {
        return new StatementImpl(tdhsNet, id, responses, protocol, timeOut, charestName);
    }

    public Statement createStatement(int hash) {
        return new StatementImpl(tdhsNet, id, responses, protocol, timeOut, charestName, hash);
    }

    public BatchStatement createBatchStatement() {
        return new BatchStatementImpl(tdhsNet, id, responses, protocol, timeOut, charestName);
    }

    // just get one
    public TDHSResponse get(@NotNull String db, @NotNull String table, @Nullable String index, @NotNull String fields[],
                            @NotNull String keys[][])
            throws TDHSException {
        return createStatement().get(db, table, index, fields, keys);
    }


    public TDHSResponse get(@NotNull String db, @NotNull String table, @Nullable String index, @NotNull String fields[],
                            @NotNull String keys[][], @NotNull TDHSCommon.FindFlag findFlag, int start,
                            int limit, @Nullable Filter filters[])
            throws TDHSException {
        return createStatement().get(db, table, index, fields, keys, findFlag, start, limit, filters);
    }

    public TDHSResponse get(@NotNull Get get) throws TDHSException {
        return createStatement().get(get);
    }

    public TDHSResponse delete(@NotNull String db, @NotNull String table, String index, @NotNull String keys[][],
                               @NotNull TDHSCommon.FindFlag findFlag, int start,
                               int limit, @Nullable Filter filters[])
            throws TDHSException {
        return createStatement().delete(db, table, index, keys, findFlag, start, limit, filters);
    }

    public TDHSResponse delete(@NotNull Get get) throws TDHSException {
        return createStatement().delete(get);
    }

    public TDHSResponse update(@NotNull String db, @NotNull String table, String index, @NotNull String fields[],
                               @NotNull ValueEntry valueEntry[],
                               @NotNull String keys[][],
                               @NotNull TDHSCommon.FindFlag findFlag, int start,
                               int limit, @Nullable Filter filters[])
            throws TDHSException {
        return createStatement().update(db, table, index, fields, valueEntry, keys, findFlag, start, limit, filters);
    }

    public TDHSResponse update(@NotNull Update update) throws TDHSException {
        return createStatement().update(update);
    }


    public TDHSResponse insert(@NotNull String db, @NotNull String table, @NotNull String fields[],
                               @NotNull String values[])
            throws TDHSException {
        return createStatement().insert(db, table, fields, values);
    }

    public TDHSResponse insert(@NotNull Insert insert) throws TDHSException {
        return createStatement().insert(insert);
    }

    public Query query() {
        return createStatement().query();
    }

    public com.taobao.adfs.database.tdhsocket.client.easy.Insert insert() {
        return createStatement().insert();
    }

    public void shutdown() {
        tdhsNet.release();
    }

}
