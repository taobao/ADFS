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

package com.taobao.adfs.database.tdhsocket.client.statement;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSBatchException;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.exception.TDHSTimeoutException;
import com.taobao.adfs.database.tdhsocket.client.net.TDHSNet;
import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;
import com.taobao.adfs.database.tdhsocket.client.protocol.TDHSProtocol;
import com.taobao.adfs.database.tdhsocket.client.request.Get;
import com.taobao.adfs.database.tdhsocket.client.request.RequestWithCharest;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponseEnum;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 12-2-21 下午1:31
 */
public class BatchStatementImpl extends StatementImpl implements BatchStatement {

    private final List<internal_struct> batchRequest = new LinkedList<internal_struct>();

    protected int batchTimeOut = -1;


    public BatchStatementImpl(TDHSNet tdhsNet, AtomicLong id,
                              ConcurrentHashMap<Long, ArrayBlockingQueue<BasePacket>> responses, TDHSProtocol protocol,
                              int timeOut, String charestName) {
        super(tdhsNet, id, responses, protocol, timeOut, charestName);
    }

    @Override public TDHSResponse get(@NotNull Get get) throws TDHSException {
        throw new UnsupportedOperationException("Batch is not support GET operation!");
    }

    public TDHSResponse[] commit() throws TDHSException {
        ByteArrayOutputStream retData = new ByteArrayOutputStream(2 * 1024);
        long headerId = id.getAndIncrement();
        try {
            try {
                for (internal_struct is : batchRequest) {
                    retData.write(is.getPacket().toByteArray());
                    responses.put(is.getPacket().getSeqId(), new ArrayBlockingQueue<BasePacket>(1));
                }
            } catch (IOException e) {
                throw new TDHSException(e);
            }
            BasePacket headerPacket =
                    new BasePacket(TDHSCommon.RequestType.BATCH, headerId, batchRequest.size(),
                            retData.toByteArray());
            ArrayBlockingQueue<BasePacket> queue = new ArrayBlockingQueue<BasePacket>(1);
            responses.put(headerId, queue);
            tdhsNet.write(headerPacket);
            return do_real_response(queue);
        } finally {
            responses.remove(headerId);
            for (internal_struct is : batchRequest) {
                responses.remove(is.getPacket().getSeqId());
            }

        }
    }

    public void setTimeOut(int timeOut) {
        this.batchTimeOut = timeOut;
    }

    protected int getTimeOut() {
        if (this.batchTimeOut > 0) {
            return this.batchTimeOut;
        } else {
            return this.timeOut;
        }
    }

    private TDHSResponse[] do_real_response(ArrayBlockingQueue<BasePacket> queue) throws TDHSException {
        BasePacket ret = null;
        try {
            ret = queue.poll(getTimeOut(), TimeUnit.MILLISECONDS);
            if (ret == null) {
                throw new TDHSTimeoutException("TimeOut");
            }
            if (!TDHSResponseEnum.ClientStatus.MULTI_STATUS.equals(ret.getClientStatus())) {
                if (ret.getClientStatus() != null && ret.getClientStatus().getStatus() >= 400 &&
                        ret.getClientStatus().getStatus() < 600) {
                    throw new TDHSBatchException(
                            new TDHSResponse(ret.getClientStatus(), null, ret.getData(), charestName));
                } else {
                    throw new TDHSException("unknown response code! [" + (ret.getClientStatus() != null ?
                            String.valueOf(ret.getClientStatus().getStatus()) : "") + "]");
                }
            }
            if (ret.getBatchNumber() != batchRequest.size()) {
                throw new TDHSException(
                        "unmatch batch size! request is[" + String.valueOf(batchRequest.size()) + "], response is [" +
                                String.valueOf(ret.getBatchNumber()) + "]");
            }

            TDHSResponse result[] = new TDHSResponse[batchRequest.size()];
            int i = 0;
            for (internal_struct is : batchRequest) {
                result[i++] =
                        do_response(responses.get(is.getPacket().getSeqId()), is.getFieldNames(), is.getCharestName());
            }
            return result;
        } catch (InterruptedException e) {
            throw new TDHSException(e);
        }
    }

    @Override
    protected TDHSResponse sendRequest(TDHSCommon.RequestType type, RequestWithCharest request, List<String> fieldNames)
            throws TDHSException {
        if (request == null) {
            throw new IllegalArgumentException("request can't be NULL!");
        }
        if (StringUtils.isBlank(request.getCharestName())) {
            //use default charestName
            request.setCharestName(this.charestName);
        }
        byte data[] = protocol.encode(request);
        BasePacket packet = new BasePacket(type, id.getAndIncrement(), data);
        batchRequest.add(new internal_struct(packet, fieldNames, request.getCharestName()));
        return null;
    }


    private class internal_struct {
        private BasePacket packet;

        private List<String> fieldNames;

        private String charestName;

        private internal_struct(BasePacket packet, List<String> fieldNames, String charestName) {
            this.packet = packet;
            this.fieldNames = fieldNames;
            this.charestName = charestName;
        }

        public BasePacket getPacket() {
            return packet;
        }

        public List<String> getFieldNames() {
            return fieldNames;
        }

        public String getCharestName() {
            return charestName;
        }
    }

}
