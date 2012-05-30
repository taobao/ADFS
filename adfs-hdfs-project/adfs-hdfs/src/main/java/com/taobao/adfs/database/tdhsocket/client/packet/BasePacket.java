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

package com.taobao.adfs.database.tdhsocket.client.packet;

import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponseEnum;
import com.taobao.adfs.database.tdhsocket.client.util.ByteOrderUtil;
import com.taobao.adfs.database.tdhsocket.client.util.ConvertUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-10-31 下午3:57
 */
public class BasePacket {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public final static int TDHS_MAGIC_CODE = 0xFFFFFFFF;

    public final static int TDHS_MAGIC_CODE_SIZE = 4;

    public final static int TDH_SOCKET_COMAND_LENGTH = 4;

    public final static int TDH_SOCKET_ID_LENGTH = 4;

    public final static int TDH_SOCKET_REVERSE_LENGTH = 4;

    public final static int TDH_SOCKET_SIZE_LENGTH = 4;

    public final static int TDH_SOCKET_HEADER_LENGTH =
            TDHS_MAGIC_CODE_SIZE + TDH_SOCKET_COMAND_LENGTH + TDH_SOCKET_ID_LENGTH + TDH_SOCKET_REVERSE_LENGTH +
                    TDH_SOCKET_SIZE_LENGTH;

    private TDHSCommon.RequestType commandIdOrResponseCode;

    private TDHSResponseEnum.ClientStatus clientStatus;

    private long seqId;

    private long reserved;

    private byte data[];


    public BasePacket(TDHSCommon.RequestType commandIdOrResponseCode, long seqId, byte[] data) {
        this.commandIdOrResponseCode = commandIdOrResponseCode;
        this.seqId = seqId;
        this.reserved = 0;
        this.data = data;
    }

    public BasePacket(TDHSCommon.RequestType commandIdOrResponseCode, long seqId, long reserved, byte[] data) {
        this.commandIdOrResponseCode = commandIdOrResponseCode;
        this.seqId = seqId;
        this.reserved = reserved;
        this.data = data;
    }


    public BasePacket(TDHSResponseEnum.ClientStatus clientStatus, long seqId, byte[] data) {
        this.clientStatus = clientStatus;
        this.seqId = seqId;
        this.data = data;
    }

    public BasePacket(TDHSResponseEnum.ClientStatus clientStatus, long seqId, long reserved, byte[] data) {
        this.clientStatus = clientStatus;
        this.seqId = seqId;
        this.data = data;
        this.reserved = reserved;
    }

    public TDHSCommon.RequestType getCommandIdOrResponseCode() {
        return commandIdOrResponseCode;
    }

    public void setCommandIdOrResponseCode(TDHSCommon.RequestType commandIdOrResponseCode) {
        this.commandIdOrResponseCode = commandIdOrResponseCode;
    }


    public TDHSResponseEnum.ClientStatus getClientStatus() {
        return clientStatus;
    }

    public void setClientStatus(TDHSResponseEnum.ClientStatus clientStatus) {
        this.clientStatus = clientStatus;
    }

    public long getSeqId() {
        return seqId;
    }

    public void setSeqId(long seqId) {
        this.seqId = seqId;
    }

    public long getBatchNumber() {
        return reserved;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] toByteArray() {
        byte result[] = new byte[TDH_SOCKET_HEADER_LENGTH + data.length];
        int i = 0;
        //write magic code
        long mcode = TDHS_MAGIC_CODE;
        ByteOrderUtil.writeIntToNet(result, i, mcode);
        i += 4;
        //write request code
        long rcode = commandIdOrResponseCode.getValue();
        ByteOrderUtil.writeIntToNet(result, i, rcode);
        i += 4;
        //write  seq_id
        long tempId = seqId;
        ByteOrderUtil.writeIntToNet(result, i, tempId);
        i += 4;

        //write batch_number
        long tempBatch = reserved;
        ByteOrderUtil.writeIntToNet(result, i, tempBatch);
        i += 4;

        //write length
        long len = data.length;
        ByteOrderUtil.writeIntToNet(result, i, len);
        i += 4;

        //write data
        for (; i < TDH_SOCKET_HEADER_LENGTH + data.length; i++) {
            result[i] = data[i - TDH_SOCKET_HEADER_LENGTH];
        }

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("Request hex:[");
            for (byte b : result) {
                sb.append(ConvertUtil.toHex(b));
                sb.append(" ");
            }
            sb.append("]");
            logger.debug(sb.toString());
        }

        return result;
    }
}
