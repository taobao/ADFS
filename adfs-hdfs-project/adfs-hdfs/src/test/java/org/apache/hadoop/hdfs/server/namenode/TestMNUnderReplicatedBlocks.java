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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;

import com.taobao.adfs.state.StateManager;

import junit.framework.TestCase;

public class TestMNUnderReplicatedBlocks extends TestCase {
  private StateManager state;
  private Configuration conf;

  public TestMNUnderReplicatedBlocks(String name) throws Exception {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
    state = new StateManager(conf);
  }

  public void testSetrepIncWithUnderReplicatedBlocks1() throws Exception {
    conf.set("dfs.namenode.port.list","0,0");
    final short repFactor = 2;  
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, repFactor+1, 0, true, null);
    internalTestSetrepIncWithUnderReplicatedBlocks(cluster, repFactor, conf);
  }
  
  public void testSetrepIncWithUnderReplicatedBlocks2() throws Exception {
    conf.set("dfs.namenode.port.list","0,0");
    final short REPLICATION_FACTOR = 2;  
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, REPLICATION_FACTOR+1, 1, true, null);
    internalTestSetrepIncWithUnderReplicatedBlocks(cluster, REPLICATION_FACTOR, conf);
  }
  
  public void testSetrepIncWithUnderReplicatedBlocks3() throws Exception {
    conf.set("dfs.namenode.port.list","0,0");
    final short REPLICATION_FACTOR = 2;  
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, REPLICATION_FACTOR, 0, true, null);
    cluster.startDataNodes(conf, 1, 1, true, null, null, null);
    internalTestSetrepIncWithUnderReplicatedBlocks(cluster, REPLICATION_FACTOR, conf);
  }
  
  private void internalTestSetrepIncWithUnderReplicatedBlocks(MiniMNDFSCluster cluster, 
		  short repFactor, Configuration conf) throws Exception {
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    cluster.waitDatanodeDie();
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, repFactor, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, repFactor);
      
      // remove one replica from the blocksMap so block becomes under-replicated
      // but the block does not get put into the under-replicated blocks queue
      FSNamesystem namesystem = cluster.getNameNode(0).namesystem;
      Block b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);
      List<Integer> nodes = state.getStoredBlockBy(b.getBlockId()).getDatanodeIds();
      if(nodes != null && nodes.size() > 0){
        DatanodeDescriptor dn = namesystem.getDataNodeDescriptorByID((int)nodes.get(0));
	      namesystem.addToInvalidates(b, dn);
	      state.removeBlockReplicationOnDatanodeBy(nodes.get(0), b.getBlockId());
      }
      
      // increment this file's replication factor
      FsShell shell = new FsShell(conf);
      assertEquals(0, shell.run(new String[]{
          "-setrep", "-w", Integer.toString(1+repFactor), FILE_NAME}));
    } finally {
      cluster.shutdown();
    }
    
  }

}
