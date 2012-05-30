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

import com.taobao.adfs.database.tdhsocket.client.exception.TDHSException;
import com.taobao.adfs.database.tdhsocket.client.net.AbstractTDHSNet;
import com.taobao.adfs.database.tdhsocket.client.net.ConnectionPool;
import com.taobao.adfs.database.tdhsocket.client.net.NetParameters;
import com.taobao.adfs.database.tdhsocket.client.net.TDHSNet;
import com.taobao.adfs.database.tdhsocket.client.packet.BasePacket;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-26 下午4:21
 */
public class TDHSNetForNetty extends AbstractTDHSNet<Channel> implements TDHSNet {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private ClientBootstrap bootstrap;

    private boolean isReleaseing = false;


    public void write(BasePacket packet) throws TDHSException {
        Channel channel = connectionPool.get();
        if (channel == null) {
            throw new TDHSException("no available connection! maybe server has someting error!");
        }
        channel.write(packet);
    }

    protected void _initNet(NetParameters parameters, BasePacket shakeHandPacket,
                            Map<Long, ArrayBlockingQueue<BasePacket>> responses) {
        parameters.isVaild();
        bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(
                new TDHSPiplelineFactoty(shakeHandPacket, responses, this));
    }

    @Override protected Channel _connect(InetSocketAddress address) {
        ChannelFuture future = bootstrap.connect(address);
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            logger.error("connect failed!");
            return null;
        } else {
            return channel;
        }
    }

    protected void _release() {
        isReleaseing = true;
        connectionPool.close(new ConnectionPool.Handler<Channel>() {
            public void execute(Channel channel) {
                channel.close();
            }
        });
        bootstrap.releaseExternalResources();
    }

    public void needCloseChannel(Channel channel) {
        if (isReleaseing) {
            //Releaseing will close channel by self
            return;
        }
        boolean ret = connectionPool.remove(channel);
        if (channel.isOpen()) {
            channel.close();
        }
        if (ret) {
            needConnectionNumber.incrementAndGet();
        }
    }
}
