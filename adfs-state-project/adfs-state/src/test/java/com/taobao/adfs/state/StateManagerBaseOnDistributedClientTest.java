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

package com.taobao.adfs.state;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.BeforeClass;

import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.distributed.DistributedMonitor;
import com.taobao.adfs.distributed.DistributedServer;
import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.file.FileRepositoryTest;
import com.taobao.adfs.state.StateManager;
import com.taobao.adfs.state.internal.StateManagerInternal;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerBaseOnDistributedClientTest extends StateManagerTest {
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    startZookeeperServer();
    startCluster();
  }

  public static void startZookeeperServer() throws Exception {
    String simpleClassName = StateManagerBaseOnDistributedClientTest.class.getSimpleName();
    final String dataPath = "target/" + simpleClassName + "/zookeeperServerData";
    Utilities.mkdirs(dataPath, true);
    new Thread() {
      public void run() {
        QuorumPeerMain.main(new String[] { "21811", dataPath });
      }
    }.start();
    Thread.sleep(3000);
  }

  public static void startCluster() throws Exception {
    String simpleClassName = StateManagerBaseOnDistributedClientTest.class.getSimpleName();
    Configuration conf1 = new Configuration(false);
    conf1.set("distributed.logger.conf", "target/test" + simpleClassName + "/notExistedLog4j.proproties");
    conf1
        .set(
            "distributed.logger.levels",
            "com.taobao.adfs.distributed.DistributedServer=debug,com.taobao.adfs.distributed.DistributedClient=debug,com.taobao.adfs.distributed.DistributedData=debug");
    conf1.set("distributed.manager.address", "localhost:21811");
    conf1.set("distributed.manager.election.delay.time", "0");
    conf1.set("distributed.server.name", "localhost:" + 55010);
    conf1.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf1.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf1.set("distributed.data.class.name", StateManagerInternal.class.getName());
    conf1.set("distributed.data.client.class.name", StateManager.class.getName());
    conf1.set("distributed.data.path", "target/test" + simpleClassName + "/" + conf1.get("distributed.server.name"));
    conf1.set("distributed.data.restore.increment.version.gap.max", Long.toString(Long.MAX_VALUE));
    conf1.set("distributed.server.handler.number", "5");
    conf1.set("distributed.data.format", "true");
    conf1.setInt("file.cache.capacity", 1);
    conf1.set("distributed.logger.conf.log4j.rootLogger", "INFO,console");
    conf1.set("distributed.metrics.enable", "false");
    Configuration conf2 = new Configuration(conf1);
    conf2.set("distributed.server.name", "localhost:" + 55011);
    conf2.set("distributed.data.path", "target/test" + simpleClassName + "/" + conf2.get("distributed.server.name"));

    // start two server
    DistributedServer.startServer(conf1, false);
    Thread.sleep(1000);
    DistributedServer.startServer(conf2, false);
    Thread.sleep(5000);
    assertThat(new DistributedMonitor(conf1).getServerType(), is(ServerType.MASTER));
    assertThat(new DistributedMonitor(conf2).getServerType(), is(ServerType.SLAVE));

    // create client
    stateManager = new StateManager(conf1);
  }
}
