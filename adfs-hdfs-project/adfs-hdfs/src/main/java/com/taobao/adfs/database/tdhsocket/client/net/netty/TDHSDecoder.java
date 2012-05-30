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

package com.taobao.adfs.database.tdhsocket.client.net.netty;

import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponseEnum;
import com.taobao.adfs.database.tdhsocket.client.util.ConvertUtil;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-10-31 下午2:50
 */
public class TDHSDecoder extends FrameDecoder {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Method decode ...
     *
     * @param ctx     of type ChannelHandlerContext
     * @param channel of type Channel
     * @param buffer  of type ChannelBuffer
     *
     * @return Object
     *
     * @throws Exception when
     */
    @Override protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
            throws Exception {
        if (buffer.readableBytes() < BasePacket.TDH_SOCKET_HEADER_LENGTH) {
            return null;
        }

        //read length
        long dataLength = buffer.getUnsignedInt(
                buffer.readerIndex() + BasePacket.TDH_SOCKET_HEADER_LENGTH - BasePacket.TDH_SOCKET_SIZE_LENGTH);
        if (buffer.readableBytes() < BasePacket.TDH_SOCKET_HEADER_LENGTH + dataLength) {
            return null;
        }

        //read magic code
        //long magicCode = buffer.getUnsignedInt(buffer.readerIndex());

        //read resp code
        long response = buffer.getUnsignedInt(buffer.readerIndex() + BasePacket.TDHS_MAGIC_CODE_SIZE);

        //read seq id
        long seqId = buffer.getUnsignedInt(
                buffer.readerIndex() + BasePacket.TDHS_MAGIC_CODE_SIZE + BasePacket.TDH_SOCKET_COMAND_LENGTH);

        long reserved = buffer.getUnsignedInt(
                buffer.readerIndex() + BasePacket.TDHS_MAGIC_CODE_SIZE + BasePacket.TDH_SOCKET_COMAND_LENGTH +
                        BasePacket.TDH_SOCKET_ID_LENGTH);

        byte data[] = new byte[(int) dataLength];
        int dataOffset = buffer.readerIndex() + BasePacket.TDH_SOCKET_HEADER_LENGTH;
        buffer.getBytes(dataOffset, data);
        buffer.readerIndex(dataOffset + (int) dataLength);

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("Response data hex:[");
            for (byte b : data) {
                sb.append(ConvertUtil.toHex(b));
                sb.append(" ");
            }
            sb.append("]");
            logger.debug(sb.toString());
        }
        return new BasePacket(TDHSResponseEnum.ClientStatus.valueOf((int) response), seqId, reserved, data);
    }

//    private long getUnsignInt(final ChannelBuffer buffer, final int pos) {
//        long ret = 0;
//        byte data[] = new byte[4];
//        buffer.getBytes(pos, data);
//        for (int i = 0; i < 4; i++) {
//            ret += ((data[i] & 0xFF) << (8 * i));
//        }
//        return ret;
//    }
}
