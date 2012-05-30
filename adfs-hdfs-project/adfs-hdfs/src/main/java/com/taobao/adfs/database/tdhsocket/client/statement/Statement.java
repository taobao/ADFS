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

package com.taobao.adfs.database.tdhsocket.client.statement;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.easy.Query;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.request.*;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-2-21 上午9:42
 */
public interface Statement {
    // just get one
    TDHSResponse get(@NotNull String db, @NotNull String table, @Nullable String index, @NotNull String fields[],
                     @NotNull String keys[][])
            throws TDHSException;

    TDHSResponse get(@NotNull String db, @NotNull String table, @Nullable String index, @NotNull String fields[],
                     @NotNull String keys[][], @NotNull TDHSCommon.FindFlag findFlag, int start, int limit,
                     @Nullable Filter filters[])
            throws TDHSException;

    TDHSResponse get(@NotNull Get get) throws TDHSException;

    TDHSResponse delete(@NotNull String db, @NotNull String table, String index, @NotNull String keys[][],
                        @NotNull TDHSCommon.FindFlag findFlag, int start,
                        int limit, @Nullable Filter filters[])
            throws TDHSException;

    TDHSResponse delete(@NotNull Get get) throws TDHSException;

    TDHSResponse update(@NotNull String db, @NotNull String table, String index, @NotNull String fields[],
                        @NotNull ValueEntry valueEntry[],
                        @NotNull String keys[][],
                        @NotNull TDHSCommon.FindFlag findFlag, int start,
                        int limit, @Nullable Filter filters[])
            throws TDHSException;

    TDHSResponse update(@NotNull Update update) throws TDHSException;

    TDHSResponse insert(@NotNull String db, @NotNull String table, @NotNull String fields[],
                        @NotNull String values[])
            throws TDHSException;

    TDHSResponse insert(@NotNull Insert insert) throws TDHSException;

    Query query();

    com.taobao.adfs.database.tdhsocket.client.easy.Insert insert();
}
