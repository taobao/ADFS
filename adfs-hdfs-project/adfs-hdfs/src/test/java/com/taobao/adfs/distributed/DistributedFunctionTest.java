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

package com.taobao.adfs.distributed;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.distributed.example.ExampleData;
import com.taobao.adfs.distributed.example.ExampleProtocol;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public class DistributedFunctionTest {
  static String simpleClassName = DistributedFunctionTest.class.getSimpleName();
  static Configuration conf1;
  static Configuration conf2;

  @BeforeClass
  static public void setup() throws Exception {
    final String dataPath = "target/" + simpleClassName + "/zookeeperServerData";
    Utilities.mkdirs(dataPath, true);
    new Thread() {
      public void run() {
        QuorumPeerMain.main(new String[] { "21810", dataPath });
      }
    }.start();
    Thread.sleep(3000);
  }

  @Test
  public void clusterTest() throws Exception {
    String simpleClassName = getClass().getSimpleName();
    conf1 = new Configuration(false);
    conf1.set("distributed.logger.conf", "target/test" + simpleClassName + "/notExistedLog4j.proproties");
    conf1
        .set(
            "distributed.logger.levels",
            "com.taobao.adfs.distributed.DistributedServer=debug,com.taobao.adfs.distributed.DistributedClient=debug,com.taobao.adfs.distributed.DistributedData=debug");
    conf1.set("distributed.manager.address", "localhost:21810");
    conf1.set("distributed.manager.election.delay.time", "0");
    conf1.set("distributed.manager.timeout", "3000");
    conf1.set("distributed.server.name", "localhost:" + 55000);
    conf1.set("distributed.data.class.name", ExampleData.class.getName());
    conf1.set("distributed.data.path", "target/test" + simpleClassName + "/" + conf1.get("distributed.server.name"));
    conf1.set("distributed.data.format", "true");
    conf1.set("ipc.client.connect.max.retries", "0");
    conf1.set("ipc.client.connection.number.per.address", "2");
    conf1.set("distributed.server.handler.number", "5");
    conf1.set("distributed.metrics.mbean.exception.log", "false");
    conf1.set("distributed.logger.conf.log4j.rootLogger", "INFO,console");
    conf1.set("distributed.metrics.enable", "false");
    conf2 = new Configuration(conf1);
    conf2.set("distributed.server.name", "localhost:" + 55001);
    conf2.set("distributed.data.path", "target/test" + simpleClassName + "/" + conf2.get("distributed.server.name"));
    DistributedServer.configLogger(conf1);
    long version = 0;
    ExampleData data1 = new ExampleData(conf1);
    ExampleData data2 = new ExampleData(conf2);

    // test start two servers
    new DistributedServer(data1).openData();
    Thread.sleep(3000);
    new DistributedServer(data2).openData();
    Thread.sleep(5000);
    DistributedMonitor distributedMonitorMaster = new DistributedMonitor(conf1);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    DistributedMonitor distributedMonitorSlave = new DistributedMonitor(conf2);
    assertThat(distributedMonitorSlave.getServerType(), is(ServerType.SLAVE));
    assertThat(distributedMonitorSlave.getDataVersion(), is(version));

    // test stop slave
    assertThat(distributedMonitorSlave.stop(), is(true));
    new DistributedManager(conf2).unregister(conf2.get("distributed.server.name"));
    Thread.sleep(5000);
    Exception e = null;
    try {
      distributedMonitorSlave.getServerType();
    } catch (IOException ioe) {
      e = ioe;
    }
    assertThat(e, is(ConnectException.class));

    // test write one server
    ExampleProtocol client = (ExampleProtocol) DistributedClient.getClient(conf1);
    client.write("I");
    ++version;
    distributedMonitorMaster = new DistributedMonitor(conf1);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    assertThat((String) distributedMonitorMaster.remoteFieldMethod("data", "read"), is("I"));

    // test read one server
    assertThat(client.read(), is("I"));

    // test add second server
    new DistributedServer(data2).openData();
    Thread.sleep(5000);
    distributedMonitorMaster = new DistributedMonitor(conf1);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    distributedMonitorSlave = new DistributedMonitor(conf2);
    assertThat(distributedMonitorSlave.getServerType(), is(ServerType.SLAVE));
    assertThat(distributedMonitorSlave.getDataVersion(), is(version));
    assertThat((String) distributedMonitorSlave.remoteFieldMethod("data", "read"), is("I"));

    // test write two server
    client.write("am");
    ++version;
    distributedMonitorMaster = new DistributedMonitor(conf1);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    assertThat((String) distributedMonitorMaster.remoteFieldMethod("data", "read"), is("am"));
    distributedMonitorSlave = new DistributedMonitor(conf2);
    assertThat(distributedMonitorSlave.getServerType(), is(ServerType.SLAVE));
    assertThat(distributedMonitorSlave.getDataVersion(), is(version));
    assertThat((String) distributedMonitorSlave.remoteFieldMethod("data", "read"), is("am"));

    // test read two servers
    assertThat(client.read(), is("am"));

    // test stop master
    distributedMonitorMaster = new DistributedMonitor(conf1);
    distributedMonitorMaster.stop();
    new DistributedManager(conf1).unregister(conf1.get("distributed.server.name"));
    Thread.sleep(5000);
    assertThat(client.read(), is("am"));
    distributedMonitorMaster = new DistributedMonitor(conf2);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));

    // test write new master
    client.write("jiwan");
    ++version;
    distributedMonitorMaster = new DistributedMonitor(conf2);
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    assertThat((String) distributedMonitorMaster.remoteFieldMethod("data", "read"), is("jiwan"));

    // test add old master
    new DistributedServer(data1).openData();
    Thread.sleep(5000);
    assertThat(client.read(), is("jiwan"));
    distributedMonitorSlave = new DistributedMonitor(conf1);
    assertThat(distributedMonitorSlave.getServerType(), is(ServerType.SLAVE));
    assertThat(distributedMonitorSlave.getDataVersion(), is(version));
    assertThat((String) distributedMonitorSlave.remoteFieldMethod("data", "read"), is("jiwan"));
    distributedMonitorMaster = new DistributedMonitor(conf2);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    assertThat((String) distributedMonitorMaster.remoteFieldMethod("data", "read"), is("jiwan"));

    // test stop slave
    distributedMonitorSlave = new DistributedMonitor(conf1);
    distributedMonitorSlave.stop();
    new DistributedManager(conf1).unregister(conf1.get("distributed.server.name"));
    Thread.sleep(5000);
    assertThat(client.read(), is("jiwan"));
    distributedMonitorMaster = new DistributedMonitor(conf2);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.MASTER));

    // test add old slave
    new DistributedServer(data1).openData();
    Thread.sleep(5000);
    assertThat(client.read(), is("jiwan"));
    assertThat(client.write("@taobao.com"), is("@taobao.com"));
    ++version;
    distributedMonitorMaster = new DistributedMonitor(conf1);
    assertThat(distributedMonitorMaster.getServerType(), is(ServerType.SLAVE));
    assertThat(distributedMonitorMaster.getDataVersion(), is(version));
    assertThat((String) distributedMonitorMaster.remoteFieldMethod("data", "read"), is("@taobao.com"));
    distributedMonitorSlave = new DistributedMonitor(conf2);
    assertThat(distributedMonitorSlave.getServerType(), is(ServerType.MASTER));
    assertThat(distributedMonitorSlave.getDataVersion(), is(version));
    assertThat((String) distributedMonitorSlave.remoteFieldMethod("data", "read"), is("@taobao.com"));

    DistributedClient.close(client);
    distributedMonitorSlave.stop();
    distributedMonitorMaster.stop();
  }
}
