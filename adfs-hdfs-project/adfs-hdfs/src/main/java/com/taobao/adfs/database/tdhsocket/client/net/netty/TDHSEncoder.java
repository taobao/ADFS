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

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.ByteOrder;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-10-31 下午2:50
 */
public class TDHSEncoder extends OneToOneEncoder {
    @Override protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if (!(msg instanceof BasePacket)) {
            return msg;
        }
        return ChannelBuffers.wrappedBuffer(ByteOrder.BIG_ENDIAN, ((BasePacket) msg).toByteArray());
    }
}
