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
package org.apache.hadoop.hdfs.server.namenode;

import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.NumberReplicas;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeActivtyMBean;

import com.taobao.adfs.state.StateManager;

import junit.framework.TestCase;

/**
 * Test if live nodes count per node is correct 
 * so NN makes right decision for under/over-replicated blocks
 */
public class TestSNNodeCount extends TestCase {
  static final Log LOG = LogFactory.getLog(TestSNNodeCount.class);
  private Configuration conf;
  private StateManager state;

  public TestSNNodeCount(String name) throws Exception {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    state = new StateManager(conf);
  }

  public void xtestInvalidateMultipleReplicas() throws Exception {
    conf.set("dfs.namenode.port.list","0");
    MiniMNDFSCluster cluster =
      new MiniMNDFSCluster(conf, 5, true, null);
    final int FILE_LEN = 123;
    final String pathStr = "/testInvalidateMultipleReplicas";
    FileSystem fs = null;
    try {
      fs = cluster.getFileSystem();
      Path path = new Path(pathStr);
      cluster.waitActive();
      // create a small file on 3 nodes
      DFSTestUtil.createFile(fs, path, 123, (short)3, 0);
      cluster.waitDatanodeDie();
      DFSTestUtil.waitReplication(fs, path, (short)3);
      NameNode nn = cluster.getNameNode(0);
      LocatedBlocks located = nn.getBlockLocations(pathStr, 0, FILE_LEN);
      
      // Get the original block locations
      List<LocatedBlock> blocks = located.getLocatedBlocks();
      LocatedBlock firstBlock = blocks.get(0);
      
      DatanodeInfo[] locations = firstBlock.getLocations();
      LOG.info("^^^^^^ good blocks replication is : " + locations.length);
      assertEquals("Should have 3 good blocks", 3, locations.length);
      LOG.info("^^^^^^ stallReplication");
      nn.getNamesystem().stallReplicationWork();
      
      DatanodeInfo[] badLocations = new DatanodeInfo[2];
      badLocations[0] = locations[0];
      badLocations[1] = locations[1];
      
      // Report some blocks corrupt
      LocatedBlock badLBlock = new LocatedBlock(
          firstBlock.getBlock(), badLocations);
      LOG.info("^^^^^^ corrupt 2 blocks");
      nn.reportBadBlocks(new LocatedBlock[] {badLBlock});
      
      LOG.info("^^^^^^ restartrestartReplicationWork ");
      nn.getNamesystem().restartReplicationWork();
      
      DFSTestUtil.waitReplication(fs, path, (short)3);
      NumberReplicas num = nn.getNamesystem().countNodes(
          firstBlock.getBlock());
      assertEquals(0, num.corruptReplicas());

    } finally {
      if(fs !=null) fs.close();
      cluster.shutdown();
    }
  }
  
  public void testNodeCount() throws Exception {
    // start a mini dfs cluster of 2 nodes
    short REPLICATION_FACTOR = (short)2;
    conf.set("dfs.namenode.port.list","0");
    MiniMNDFSCluster cluster = 
      new MiniMNDFSCluster(conf, REPLICATION_FACTOR, true, null);
    try {
      FSNamesystem namesystem = cluster.getNameNode(0).namesystem;
      FileSystem fs = cluster.getFileSystem();
      
      // populate the cluster with a one block file
      final Path FILE_PATH = new Path("/testfile");
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
//      cluster.waitDatanodeDie();
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      Block block = DFSTestUtil.getFirstBlock(fs, FILE_PATH);

      // keep a copy of all datanode descriptor
      DatanodeDescriptor[] datanodes = (DatanodeDescriptor[])
         namesystem.heartbeats.toArray(new DatanodeDescriptor[REPLICATION_FACTOR]);
      
      // start two new nodes
      cluster.startDataNodes(conf, 2, true, null, null);
      cluster.waitActive(false);
      
      LOG.info("^^^^^^ Bringing down first DN");
      // bring down first datanode
      DatanodeDescriptor datanode = datanodes[0];
      DataNodeProperties dnprop = cluster.stopDataNode(datanode.getName());
      cluster.waitDatanodeDie();
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.heartbeats) {
        datanode.setLastUpdate(0); // mark it dead
        namesystem.heartbeatCheck();
      }

      LOG.info("^^^^^^ Waiting for block to be replicated");
      // the block will be replicated
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);

      LOG.info("^^^^^^ Restarting first datanode");
      // restart the first datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive(false);

      LOG.info("^^^^^^ Waiting for excess replicas to be detected-1");
      // check if excessive replica is detected
      waitForExcessReplicasToChange(namesystem, block, 0);

      LOG.info("^^^^^^ Finding a non-excess node");
      
      Collection<Integer> dnIds = state.getDatanodeList(block.getBlockId());
      assertTrue(dnIds != null && dnIds.size() > 0);
      
      // find out a non-excess node
      Iterator<DatanodeDescriptor> iter = namesystem.datanodeMap.values().iterator();
      DatanodeDescriptor nonExcessDN = null;
      while (iter.hasNext()) {
        DatanodeDescriptor dn = iter.next();
        if (!dnIds.contains(dn.getNodeid())) {
          nonExcessDN = dn;
          break;
        }
      }
      assertTrue("^^^^^^^ nonExcessDN is null", nonExcessDN != null);

      LOG.info("^^^^^^ Stopping non-excess node: " + nonExcessDN);
      // bring down non excessive datanode
      dnprop = cluster.stopDataNode(nonExcessDN.getName());
      cluster.waitDatanodeDie();
      // make sure that NN detects that the datanode is down
      synchronized (namesystem.datanodeMap) {
	      synchronized (namesystem.heartbeats) {
	    	  datanode.setLastUpdate(0); // mark it dead
	          namesystem.heartbeatCheck();
	      }
      }

      LOG.info("^^^^^^ Waiting for live replicas to hit repl factor");
      // The block should be replicated
      NumberReplicas num;
      do {
        num = namesystem.countNodes(block);
      } while (num.liveReplicas() != REPLICATION_FACTOR);
      
      LOG.info("^^^^^^ Restarting first DN");
      // restart the non excessive datanode
      cluster.restartDataNode(dnprop);
      cluster.waitActive(false);

      LOG.info("^^^^^^ Waiting for excess replicas to be detected-2");
      // check if excessive replica is detected
      waitForExcessReplicasToChange(namesystem, block, 2);
    } finally {
      cluster.shutdown();
    }
  }

  private void waitForExcessReplicasToChange(
    FSNamesystem namesystem,
    Block block,
    int oldReplicas) throws Exception
  {
    NumberReplicas num;
    long startChecking = System.currentTimeMillis();
    do {
      synchronized (namesystem) {
        num = namesystem.countNodes(block);
      }
      Thread.sleep(100);
      if (System.currentTimeMillis() - startChecking > 30000) {
        namesystem.metaSave("TestNodeCount.meta");
        LOG.warn("Dumping meta into log directory");
        fail("Timed out waiting for excess replicas to change");
      }

    } while (num.excessReplicas() == oldReplicas);
  }
    
}
