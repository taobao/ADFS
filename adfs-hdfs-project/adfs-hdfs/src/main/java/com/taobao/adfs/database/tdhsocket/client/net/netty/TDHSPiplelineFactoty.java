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

import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-10-31 下午4:04
 */
public class TDHSPiplelineFactoty implements ChannelPipelineFactory {
    private BasePacket shakeHandeMessage;

    private Map<Long, ArrayBlockingQueue<BasePacket>> responses;

    private TDHSNetForNetty tdhsNetForNetty;

    public TDHSPiplelineFactoty(BasePacket shakeHandeMessage,
                                Map<Long, ArrayBlockingQueue<BasePacket>> responses, TDHSNetForNetty tdhsNetForNetty) {
        this.responses = responses;
        this.shakeHandeMessage = shakeHandeMessage;
        this.tdhsNetForNetty = tdhsNetForNetty;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("decoder", new TDHSDecoder());
        pipeline.addLast("encoder", new TDHSEncoder());
        pipeline.addLast("handler", new TDHSClientHandler(shakeHandeMessage, responses, tdhsNetForNetty));
        return pipeline;
    }
}
