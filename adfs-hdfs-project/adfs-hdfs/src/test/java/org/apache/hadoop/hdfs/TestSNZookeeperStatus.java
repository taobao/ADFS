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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.data.Stat;

import junit.framework.TestCase;

public class TestSNZookeeperStatus extends TestCase {
  public static final Log LOG = 
    LogFactory.getLog(TestSNZookeeperStatus.class);
  private Configuration conf;
  private MiniMNDFSCluster cluster;

  public TestSNZookeeperStatus(String name) {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list", "0");
    ZKClient.getInstance(conf);
  }
  
  protected void setUp() throws Exception {
    super.setUp();
    // clean up the environment
    cleanGroups();
    //establish  the mini cluster
    cluster=new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
  }

  protected void tearDown() throws Exception {
    super.tearDown();    
    cluster.shutdown();
    Thread.sleep(2000);
  }
  
  protected void cleanGroups() throws Exception {
    List<String> namenodes = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    if(namenodes != null && namenodes.size() > 0) {
      for(String namenode : namenodes) {
        ZKClient.getInstance().delete(FSConstants.ZOOKEEPER_NAMENODE_GROUP
            + "/" + namenode, false);
      }
    }
    List<String> datanodes = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    if(datanodes != null && datanodes.size() > 0) {
      for(String datanode : datanodes) {
        ZKClient.getInstance().delete(FSConstants.ZOOKEEPER_DATANODE_GROUNP
            + "/" + datanode, false);
      }
    }
  }
 
  public void testGroupStatus() throws IOException{
    Stat stat=ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_NAMENODE_GROUP);
    assertTrue("Namenodes should be 1 but 0 instead, check zookeeper /namenode/group",stat!=null);
    stat=ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_DATANODE_GROUNP);
    assertTrue("Datanodes should be 4 but 0 instead, check zookeeper /datanode/group",stat!=null);
    List<String> namenodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_NAMENODE_GROUP, null);
    if(namenodeGroup != null) {
      for(String namenode : namenodeGroup) {
        LOG.info(">>>>>>>>>testGroupStatus: namenodegroup:: " + namenode);
      }
    }
    assertEquals(1,namenodeGroup.size());
    List<String> datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    if(datanodeGroup != null) {
      for(String datanode : datanodeGroup) {
        LOG.info(">>>>>>>>>testGroupStatus: datanodeGroup:: " + datanode);
      }
    }
    assertEquals(4,datanodeGroup.size());
  }

  public void testDatanodeStatus() throws IOException, InterruptedException{
    List<String> datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    Random r=new Random();
    DataNodeProperties dnProp=cluster.stopDataNode(r.nextInt(datanodeGroup.size()));
    cluster.waitDatanodeDie();
    datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    if(datanodeGroup != null) {
      for(String datanode : datanodeGroup) {
        LOG.info(">>>>>>>>>testDatanodeStatus: datanodeGroup(after stop):: " + datanode);
      }
    }
    assertEquals(3,datanodeGroup.size());    
    cluster.restartDataNode(dnProp);    
    datanodeGroup=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_DATANODE_GROUNP, null);
    if(datanodeGroup != null) {
      for(String datanode : datanodeGroup) {
        LOG.info(">>>>>>>>>testDatanodeStatus: datanodeGroup(after restart):: " + datanode);
      }
    }
    assertEquals(4,datanodeGroup.size());
  }
}
