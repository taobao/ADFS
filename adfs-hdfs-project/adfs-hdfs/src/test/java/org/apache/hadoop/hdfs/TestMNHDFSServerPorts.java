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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.RPC;

/**
 * This test checks correctness of port usage by hdfs components:
 * NameNode, DataNode, and SecondaryNamenode.
 * 
 * The correct behavior is:<br> 
 * - when a specific port is provided the server must either start on that port 
 * or fail by throwing {@link java.net.BindException}.<br>
 * - if the port = 0 (ephemeral) then the server should choose 
 * a free port and start on it.
 */
public class TestMNHDFSServerPorts extends TestCase {
  public static final String NAME_NODE_HOST = "localhost";
  public static final String NAME_NODE_HTTP_HOST = "0.0.0.0:";

  Configuration config;
  File hdfsDir;

  /**
   * Start the name-node.
   */
  public NameNode startNameNode(boolean format) throws IOException {
    String dataDir = System.getProperty("test.build.data", "build/test/data");
    hdfsDir = new File(dataDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
    config = new Configuration();
    URL url = this.getClass().getResource("/mini-dfs-conf.xml");
    config.addResource(url);
    config.set("dfs.name.dir", new File(hdfsDir, "name1").getPath());
    FileSystem.setDefaultUri(config, "hdfs://"+NAME_NODE_HOST + ":0");
    config.setStrings("dfs.namenode.port.list", "0");
    config.setInt("heartbeat.recheck.interval", 5000);
    config.setInt("dfs.heartbeat.interval", 2);
    config.setInt("datanodemap.update.interval", 3000);
    config.setInt("zookeeper.session.timeout", 180000);
    config.setInt("dfs.replication.pending.timeout.sec", 2);
    config.setBoolean("test.close.zookeeper", false);
    config.set("dfs.http.address", NAME_NODE_HTTP_HOST + "0");
    
    if (format) {
      NameNode.format(config);
    }

    String[] args = new String[] {};
    // NameNode will modify config with the ports it bound to
    return NameNode.createNameNode(args, config);
  }

  /**
   * Start the data-node.
   */
  public DataNode startDataNode(int index, Configuration config) 
  throws IOException {
    String dataDir = System.getProperty("test.build.data");
    File dataNodeDir = new File(dataDir, "data-" + index);
    config.set("dfs.data.dir", dataNodeDir.getPath());

    String[] args = new String[] {};
    // NameNode will modify config with the ports it bound to
    return DataNode.createDataNode(args, config);
  }

  /**
   * Stop the datanode.
   */
  public void stopDataNode(DataNode dn) {
    if (dn != null) {
      dn.shutdown();
    }
  }

  public void stopNameNode(NameNode nn) {
    if (nn != null) {
      nn.stop();
    }
  }

  public Configuration getConfig() {
    return this.config;
  }

  /**
   * Check whether the name-node can be started.
   */
  private boolean canStartNameNode(Configuration conf) throws IOException {
    NameNode nn2 = null;
    try {
      nn2 = NameNode.createNameNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    stopNameNode(nn2);
    return true;
  }

  /**
   * Check whether the data-node can be started.
   */
  private boolean canStartDataNode(Configuration conf) throws IOException {
    DataNode dn = null;
    try {
//      dn = DataNode.instantiateDataNode(new String[]{}, conf);
//      DataNode.runDatanodeDaemon(dn,true);
      dn = DataNode.createDataNode(new String[]{}, conf);
    } catch(IOException e) {
      if (e instanceof java.net.BindException)
        return false;
      throw e;
    }
    dn.shutdown();
    return true;
  }

  /**
   * Verify name-node port usage.
   */
  public void testNameNodePorts() throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode(false);

      // start another namenode on the same port
      Configuration conf2 = new Configuration(config);
      conf2.set("dfs.name.dir", new File(hdfsDir, "name2").getPath());
      NameNode.format(conf2);
      
/*      boolean started = canStartNameNode(conf2);
      assertFalse(started); // should fail

      // start on a different main port
      FileSystem.setDefaultUri(conf2, "hdfs://"+NAME_NODE_HOST + "0");
      started = canStartNameNode(conf2);
      assertFalse(started); // should fail again
*/
      // reset conf2 since NameNode modifies it
      FileSystem.setDefaultUri(conf2, "hdfs://"+NAME_NODE_HOST + ":0");
      // different http port
      conf2.set("dfs.http.address", NAME_NODE_HTTP_HOST + "0");
      boolean started = canStartNameNode(conf2);
      assertTrue(started); // should start now
      
      //start namenode with the same port of first one
      Configuration conf3 = new Configuration(config);
      conf2.set("dfs.name.dir", new File(hdfsDir, "name3").getPath());
      NameNode.format(conf3);
      FileSystem.setDefaultUri(conf3, "hdfs://"+NAME_NODE_HOST + ":" + nn.getNameNodeAddress().getPort());
      conf3.set("dfs.http.address", NAME_NODE_HTTP_HOST + "0");
      assertFalse(canStartNameNode(conf3));
    } finally {
      stopNameNode(nn);
    }
  }

  /**
   * Verify data-node port usage.
   */
  public void testDataNodePorts() throws Exception {
    NameNode nn = null;
    try {
      nn = startNameNode(true);

      // start data-node on the same port as name-node
      Configuration conf2 = new Configuration(config);
      conf2.set("dfs.data.dir", new File(hdfsDir, "data").getPath());
      conf2.set("dfs.datanode.address",
                FileSystem.getDefaultUri(config).getAuthority());
      conf2.set("dfs.datanode.http.address", NAME_NODE_HTTP_HOST + "0");
      InetSocketAddress nnAddr = nn.getNameNodeAddress();
      int nameNodePort = nnAddr.getPort();
      String str = "hdfs://" + nnAddr.getHostName() + ":" + Integer.toString(nameNodePort);
      conf2.set("dfs.namenode.rpcaddr.list", str);
      
      boolean started = canStartDataNode(conf2);
      assertFalse(started); // should fail

      // bind http server to the same port as name-node
/*      conf2.set("dfs.datanode.address", NAME_NODE_HOST + "0");
      conf2.set("dfs.datanode.http.address", 
                config.get("dfs.http.address"));
      started = canStartDataNode(conf2);
      assertFalse(started); // should fail
*/    
      // both ports are different from the name-node ones
      conf2.set("dfs.datanode.address", NAME_NODE_HOST + ":0");
      conf2.set("dfs.datanode.http.address", NAME_NODE_HTTP_HOST + "0");
      conf2.set("dfs.datanode.ipc.address", NAME_NODE_HOST + ":0");
      conf2.set("slave.host.name", NAME_NODE_HOST);
      started = canStartDataNode(conf2);
      assertTrue(started); // should start now
    } finally {
      stopNameNode(nn);
    }
  }
}
