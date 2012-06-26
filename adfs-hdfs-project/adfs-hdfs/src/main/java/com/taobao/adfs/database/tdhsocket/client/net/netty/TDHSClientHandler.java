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

package com.taobao.adfs.database.tdhsocket.client.net.netty;

import com.taobao.adfs.database.tdhsocket.client.net.ConnectionPool;
import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;

import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author <a href="mailto:wentong@taobao.com"></a>
 * 
 */
public class TDHSClientHandler extends SimpleChannelUpstreamHandler {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private BasePacket shakeHandeMessage;

    private Map<Long, ArrayBlockingQueue<BasePacket>> responses;

    private TDHSNetForNetty tdhsNetForNetty;

    public TDHSClientHandler(BasePacket shakeHandeMessage,
                             Map<Long, ArrayBlockingQueue<BasePacket>> responses, TDHSNetForNetty tdhsNetForNetty) {
        this.shakeHandeMessage = shakeHandeMessage;
        this.responses = responses;
        this.tdhsNetForNetty = tdhsNetForNetty;
    }

    @Override 
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        this.tdhsNetForNetty.addConnectedConnectionToPool(e.getChannel(), new ConnectionPool.Handler<Channel>() {
            public void execute(Channel channel) {
                channel.write(shakeHandeMessage);
            }
        });
    }


    @Override public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.warn("channelDisconnected!");
        tdhsNetForNetty.needCloseChannel(e.getChannel());
    }

    @Override public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        BasePacket packet = (BasePacket) e.getMessage();
        ArrayBlockingQueue<BasePacket> blockingQueue = responses.get(packet.getSeqId());
        if (blockingQueue != null) {
            blockingQueue.put(packet);
        }
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an com.taobao.adfs.database.tdhsocket.client.exception is raised.
        logger.error("exceptionCaught!", e.getCause());
        tdhsNetForNetty.needCloseChannel(e.getChannel());
    }
}
