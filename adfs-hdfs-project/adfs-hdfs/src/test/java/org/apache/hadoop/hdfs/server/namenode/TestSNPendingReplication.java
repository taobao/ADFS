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

import junit.framework.TestCase;

import java.io.IOException;
import java.lang.System;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;

import com.taobao.adfs.state.StateManager;

/**
 * This class tests the internals of PendingReplicationBlocks.java
 */
public class TestSNPendingReplication extends TestCase {
  public void testPendingReplication() throws IOException {
    Configuration conf = new Configuration();
    URL url = DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    StringBuilder sb = new StringBuilder();
    
    ZKClient zkClient =ZKClient.getInstance(conf);
    List<String> pendingBlks = zkClient.getChildren(FSConstants.ZOOKEEPER_PENDING_HOME, null);
    if(pendingBlks != null) {
      for(String pendingBlk : pendingBlks) {
        sb.append(FSConstants.ZOOKEEPER_PENDING_HOME).append("/").append(pendingBlk);
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }
    }
    
    int timeout = 300;		// 300 seconds
    AbsPendingReplicationBlocks pendingReplications;
    StateManager stateManager = new StateManager(conf);
    DbLockManager lockManager = new DbLockManager(stateManager, 
        conf.getLong("state.lock.expire", 60000), 
        conf.getLong("state.lock.timeout", 30000));
    pendingReplications = new ZKPendingReplicationBlocksImp(lockManager, timeout * 1000);
    
    //
    // Add 10 blocks to pendingReplications.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.add(block, i);
    }
    
    List<String> coll = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_PENDING_HOME, null);
    String path;
    byte[] data;
    for(String one:coll)
    {
    	path = FSConstants.ZOOKEEPER_PENDING_HOME + "/" + one;
        data = ZKClient.getInstance().getData(path, null);
        String str = new String(data,Charset.forName("UTF-8"));
    	System.out.println(str);
    	
    }
    
    assertEquals("Size of pendingReplications ",
                 10, pendingReplications.size());


    //
    // remove one item and reinsert it
    //
    Block blk = new Block(8, 8, 0);
    pendingReplications.remove(blk);             // removes one replica
    assertEquals("pendingReplications.getNumReplicas ",
                 7, pendingReplications.getNumReplicas(blk));

    for (int i = 0; i < 7; i++) {
      pendingReplications.remove(blk);           // removes all replicas
    }
    assertTrue(pendingReplications.size() == 9);
    pendingReplications.add(blk, 8);
    assertTrue(pendingReplications.size() == 10);

    //
    // verify that the number of replicas returned
    // are sane.
    //
    for (int i = 0; i < 10; i++) {
      Block block = new Block(i, i, 0);
      int numReplicas = pendingReplications.getNumReplicas(block);
      assertTrue(numReplicas == i);
    }

    //
    // verify that nothing has timed out so far
    //
    assertTrue(pendingReplications.getTimedOutBlocks() == null);

    //
    // Wait for one second and then insert some more items.
    //
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
    }

    for (int i = 10; i < 15; i++) {
      Block block = new Block(i, i, 0);
      pendingReplications.add(block, i);
    }
    assertTrue(pendingReplications.size() == 15);

    //
    // Wait for everything to timeout.
    //
    int loop = 0;
    while (pendingReplications.size() > 0) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      loop++;
    }
    System.out.println("Had to wait for " + loop +
                       " seconds for the lot to timeout");

    //
    // Verify that everything has timed out.
    //
    assertEquals("Size of pendingReplications ",
                 0, pendingReplications.size());
    Block[] timedOut = pendingReplications.getTimedOutBlocks();
    assertTrue("timedOut.length="+timedOut.length, timedOut != null && timedOut.length == 15);
    for (int i = 0; i < timedOut.length; i++) {
      assertTrue(timedOut[i].getBlockId() < 15);
      System.out.print("clear pending dir, delete block : "+i);
      pendingReplications.remove(timedOut[i]);
    }
  }
}
