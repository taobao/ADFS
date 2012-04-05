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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.TestSNDatanodeBlockScanner;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import com.taobao.adfs.block.BlockEntry;

import junit.framework.TestCase;

public class TestSNOverReplicatedBlocks extends TestCase {
  public static final Log LOG = LogFactory
      .getLog(TestSNOverReplicatedBlocks.class);

  /**
   * Test processOverReplicatedBlock can handle corrupt replicas fine. It make
   * sure that it won't treat corrupt replicas as valid ones thus prevents NN
   * deleting valid replicas but keeping corrupt ones.
 * @throws InterruptedException 
   */
  public void testProcesOverReplicateBlock() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 5000L);
    conf.set("dfs.replication.pending.timeout.sec", Integer.toString(2));
    URL url = DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list", "0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 3, true, null);
    FileSystem fs = cluster.getFileSystem(0);

    try {
      final Path fileName = new Path("/foo1");
      DFSTestUtil.createFile(fs, fileName, 2, (short) 3, 0L);
      cluster.waitDatanodeDie();
      DFSTestUtil.waitReplication(fs, fileName, (short) 3);
      LOG.info("^^^^^^^ file rep is to 3");

      // corrupt the block on datanode 0
      Block block = DFSTestUtil.getFirstBlock(fs, fileName);// //////////
      LOG.info("^^^^^^^^ first block name : " + block.getBlockName());
      TestSNDatanodeBlockScanner.corruptReplica(block.getBlockName(), 0);
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      cluster.waitDatanodeDie();
      LOG.info("^^^^^^^^ the datanode is die");
      // remove block scanner log to trigger block scanning
      File scanLog = new File(System.getProperty("test.build.data",
          "build/test/data"),
          "dfs/data/data1/current/dncp_block_verification.log.curr");
      assertTrue("scanlog file is not exist", scanLog.exists());
      // wait for one minute for deletion to succeed;
      for (int i = 0; !scanLog.delete(); i++) {
        assertTrue("Could not delete log file in 2 minute", i < 120);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
      }

      // restart the datanode so the corrupt replica will be detected
      LOG.info("^^^^^^^^ restart the stop datanode");
      cluster.restartDataNode(dnProps);
      DFSTestUtil.waitReplication(fs, fileName, (short) 2);
      LOG.info("^^^^^^^ file repli is 2 now");

      final DatanodeID corruptDataNode = cluster.getDataNodes().get(2).dnRegistration;
      LOG.info("^^^^^^ corruptDataNode name : " + corruptDataNode.getName());
      // final FSNamesystem namesystem = FSNamesystem.getFSNamesystem();
      final ArrayList<NameNode> namenodes = cluster.getNameNodes();
//      try {
//		Thread.sleep(60000);
//	} catch (InterruptedException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
      for (NameNode namenode : namenodes) {
        FSNamesystem namesystem = namenode.getNamesystem();
        synchronized(namesystem) {
          synchronized (namesystem.datanodeMap) {
            synchronized (namesystem.heartbeats) {
            
              // set live datanode's remaining space to be 0
              // so they will be chosen to be deleted when over-replication occurs
              for (DatanodeDescriptor datanode : namesystem.heartbeats) {
                if (!corruptDataNode.equals(datanode)) {
                  datanode.updateHeartbeat(100L, 100L, 0L, 0);
                }
              }
            }
          }
        }
     // decrease the replication factor to 1;
        namesystem.setReplication(fileName.toString(), (short) 1);

        // corrupt one won't be chosen to be excess one
        // without 4910 the number of live replicas would be 0: block gets lost
        assertEquals(1, namesystem.countNodes(block).liveReplicas());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
