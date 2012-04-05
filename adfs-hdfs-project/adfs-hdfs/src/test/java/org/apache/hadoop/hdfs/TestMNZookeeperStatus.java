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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.data.Stat;

import junit.framework.TestCase;

public class TestMNZookeeperStatus extends TestCase {
  private Configuration conf;
  private MiniMNDFSCluster cluster;

  public TestMNZookeeperStatus(String name) {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
  }
  
  protected void setUp() throws Exception {
    super.setUp();    
    //establish  the mini cluster
    cluster=new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitDatanodeDie();
  }

  protected void tearDown() throws Exception {
    super.tearDown();    
    cluster.shutdown();
    Thread.sleep(2000);
  }
  
  public void testGroupStatus() throws IOException{
    Stat stat=ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_NAMENODE_GROUP);
    assertTrue("Namenodes should be 3 but 0 instead, check zookeeper /namenode/group",stat!=null);
    stat=ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_DATANODE_GROUNP);
    assertTrue("Datanodes should be 4 but 0 instead, check zookeeper /datanode/group",stat!=null);
    
    List<String> namenodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    assertEquals(3,namenodeGroup.size());
    List<String> datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    assertEquals(4,datanodeGroup.size());
  }
  
  public void testNamenodeStatus() throws IOException{
    List<String> namenodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    Random r=new Random();
    cluster.stopNameNode(r.nextInt(namenodeGroup.size()));
    namenodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    assertEquals(2,namenodeGroup.size());
    Configuration secondConf = new Configuration(conf);
    secondConf.set("dfs.http.address", "localhost:0");
    secondConf.set("fs.default.name", "hdfs://localhost:0");
    cluster.startNewNameNode(secondConf, 0, null);
    namenodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    assertEquals(3,namenodeGroup.size());
  }
  
  public void testDatanodeStatus() throws IOException, InterruptedException{
    List<String> datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    Random r=new Random();
    DataNodeProperties dnProp=cluster.stopDataNode(r.nextInt(datanodeGroup.size()));
    cluster.waitDatanodeDie();
    datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    assertEquals(3,datanodeGroup.size());
    cluster.startDataNodes(conf, 1, false, null, null);
    datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    assertEquals(4,datanodeGroup.size());
  }
}
