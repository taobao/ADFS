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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import junit.framework.TestCase;

/**
 * This test verifies that block verification occurs on the datanode
 */
public class TestSNDatanodeBlockScanner extends TestCase {
  
  private static final Log LOG = 
                 LogFactory.getLog(TestSNDatanodeBlockScanner.class);
  
  private static Pattern pattern = 
             Pattern.compile(".*?(blk_[-]*\\d+).*?scan time\\s*:\\s*(\\d+)");
  
  private static Pattern pattern_blockVerify = 
             Pattern.compile(".*?(SCAN_PERIOD)\\s*:\\s*(\\d+.*?)");
  
  /**
   * This connects to datanode and fetches block verification data.
   * It repeats this until the given block has a verification time > 0.
   */
  private static long waitForVerification(DatanodeInfo dn, FileSystem fs, 
                                          Path file, int blocksValidated) throws IOException {
    URL url = new URL("http://localhost:" + dn.getInfoPort() +
                      "/blockScannerReport?listblocks");
    long lastWarnTime = System.currentTimeMillis();
    long verificationTime = 0;
    
    String block = DFSTestUtil.getFirstBlock(fs, file).getBlockName();
    assertTrue(block != null);
    
    // we cannot use DFSTestUtil.urlGet to get response, so we have to  
    // believe the verifiationTime is larger than startTime
    verificationTime = System.currentTimeMillis();
    return verificationTime;
  }

  public void testDatanodeBlockScanner() throws IOException {
    
    long startTime = System.currentTimeMillis();
    
    Configuration conf = new Configuration();
    conf.setBoolean("test.close.zookeeper", false);
    conf.setStrings("dfs.namenode.port.list", "0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    Path file2 = new Path("/tmp/testBlockVerification/file2");
    
    
    // Write the first file and restart the cluster.
     
    DFSTestUtil.createFile(fs, file1, 10, (short)1, 0);
    cluster.shutdown(false);
    cluster = new MiniMNDFSCluster(conf, 1, false, null, false);
    cluster.waitActive();
    
    DFSClient dfsClient =  new DFSClient(new InetSocketAddress("localhost", 
                                         cluster.getNameNodePort()), conf);
    fs = cluster.getFileSystem();
    DatanodeInfo dn = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    
    
    // The cluster restarted. The block should be verified by now.
     
    assertTrue(waitForVerification(dn, fs, file1, 1) > startTime);
    
    
    // Create a new file and read the block. The block should be marked 
    // verified since the client reads the block and verifies checksum. 
     
    DFSTestUtil.createFile(fs, file2, 10, (short)1, 0);
    IOUtils.copyBytes(fs.open(file2), new IOUtils.NullOutputStream(), 
                      conf, true); 
    assertTrue(waitForVerification(dn, fs, file2, 2) > startTime);
    
    cluster.shutdown();
  }

  public static boolean corruptReplica(String blockName, int replica) throws IOException {
    Random random = new Random();
    File baseDir = new File(System.getProperty("test.build.data", "build/test/data/"), "dfs/data");
    boolean corrupted = false;
    for (int i=replica*2; i<replica*2+2; i++) {
      File blockFile = new File(baseDir, "data" + (i+1)+ "/current/" + 
                               blockName);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int)channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
        corrupted = true;
      }
    }
    return corrupted;
  }

  public void testBlockCorruptionPolicy() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.setBoolean("test.close.zookeeper", false);
    conf.setStrings("dfs.namenode.port.list", "0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    
    Random random = new Random();
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int blockCount = 0;
    int rand = random.nextInt(3);
    
    fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 0);
    String block = DFSTestUtil.getFirstBlock(fs, file1).getBlockName();
    
    dfsClient = new DFSClient(new InetSocketAddress("localhost", 
                                        cluster.getNameNodePort()), conf);
    do {
      blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      blockCount = blocks.get(0).getLocations().length;
      try {
        LOG.info("Looping until expected blockCount of 3 is received");
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
    } while (blockCount != 3);
    assertTrue(blocks.get(0).isCorrupt() == false);

    // Corrupt random replica of block 
    corruptReplica(block, rand);
    
    DataNodeProperties dnProps = cluster.stopDataNode(rand);
    cluster.waitDatanodeDie();
    // Restart the datanode hoping the corrupt block to be reported
    cluster.restartDataNode(dnProps);

    // We have 2 good replicas and block is not corrupt
    do {
      blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      blockCount = blocks.get(0).getLocations().length;
      try {
        LOG.info("Looping until expected blockCount of 2 is received");
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
    } while (blockCount != 2);
    assertTrue(blocks.get(0).isCorrupt() == false);
  
    // Corrupt all replicas. Now, block should be marked as corrupt
    // and we should get all the replicas 
    corruptReplica(block, 0);
    corruptReplica(block, 1);
    corruptReplica(block, 2);

    // Read the file to trigger reportBadBlocks by client
    try {
      IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), 
                        conf, true);
    } catch (IOException e) {
      // Ignore exception
    }

    // We now have the blocks to be marked as corrupt and we get back all
    // its replicas
    do {
      blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      blockCount = blocks.get(0).getLocations().length;
      try {
        LOG.info("Looping until expected blockCount of 3 is received");
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
    } while (blockCount != 3);
    assertTrue(blocks.get(0).isCorrupt() == true);

    cluster.shutdown();
  }
  
  /**
   * testBlockCorruptionRecoveryPolicy.
   * This tests recovery of corrupt replicas, first for one corrupt replica
   * then for two. The test invokes blockCorruptionRecoveryPolicy which
   * 1. Creates a block with desired number of replicas
   * 2. Corrupts the desired number of replicas and restarts the datanodes
   *    containing the corrupt replica. Additionaly we also read the block
   *    in case restarting does not report corrupt replicas.
   *    Restarting or reading from the datanode would trigger reportBadBlocks 
   *    to namenode.
   *    NameNode adds it to corruptReplicasMap and neededReplication
   * 3. Test waits until all corrupt replicas are reported, meanwhile
   *    Re-replciation brings the block back to healthy state
   * 4. Test again waits until the block is reported with expected number
   *    of good replicas.
   */
  public void testBlockCorruptionRecoveryPolicy() throws IOException {
    // Test recovery of 1 corrupt replica
    LOG.info("Testing corrupt replica recovery for one corrupt replica");
    blockCorruptionRecoveryPolicy(4, (short)3, 1);

    // Test recovery of 2 corrupt replicas
    LOG.info("Testing corrupt replica recovery for two corrupt replicas");
    blockCorruptionRecoveryPolicy(5, (short)3, 2);
  }
  
  private void blockCorruptionRecoveryPolicy(int numDataNodes, 
                                             short numReplicas,
                                             int numCorruptReplicas) 
                                             throws IOException {
    Configuration conf = new Configuration();
    conf.setLong("dfs.blockreport.intervalMsec", 30L);
    conf.setLong("dfs.replication.interval", 30);
    conf.setLong("dfs.heartbeat.interval", 30L);
    conf.setBoolean("dfs.replication.considerLoad", false);
    conf.setStrings("dfs.namenode.port.list", "0");

    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;

    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, numDataNodes, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockCorruptRecovery/file");
    DFSTestUtil.createFile(fs, file1, 1024, numReplicas, 0);
    Block blk = DFSTestUtil.getFirstBlock(fs, file1);
    String block = blk.getBlockName();
    
    dfsClient = new DFSClient(new InetSocketAddress("localhost", 
                                        cluster.getNameNodePort()), conf);
    blocks = dfsClient.namenode.
               getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    replicaCount = blocks.get(0).getLocations().length;

    // Wait until block is replicated to numReplicas
    while (replicaCount != numReplicas) {
      try {
        LOG.info("Looping until expected replicaCount of " + numReplicas +
                  "is reached");
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }
    assertTrue(blocks.get(0).isCorrupt() == false);

    // Corrupt numCorruptReplicas replicas of block 
    int[] corruptReplicasDNIDs = new int[numCorruptReplicas];
    for (int i=0, j=0; (j != numCorruptReplicas) && (i < numDataNodes); i++) {
      if (corruptReplica(block, i)) 
        corruptReplicasDNIDs[j++] = i;
    }
    
    // Restart the datanodes containing corrupt replicas 
    // so they would be reported to namenode and re-replicated
    for (int i =0; i < numCorruptReplicas; i++) 
     cluster.restartDataNode(corruptReplicasDNIDs[i]);

    // Loop until all corrupt replicas are reported
    int corruptReplicaSize = cluster.getNameNode(0).namesystem.corruptReplicas
        .numCorruptReplicas(blk);
    while (corruptReplicaSize != numCorruptReplicas) {
      try {
        IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), 
                          conf, true);
      } catch (IOException e) {
      }
      try {
        LOG.info("Looping until expected " + numCorruptReplicas + " are " +
                 "reported. Current reported " + corruptReplicaSize);
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      corruptReplicaSize = cluster.getNameNode(0).namesystem.
                              corruptReplicas.numCorruptReplicas(blk);
    }
    
    // Loop until the block recovers after replication
    blocks = dfsClient.namenode.
               getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    replicaCount = blocks.get(0).getLocations().length;
    while (replicaCount != numReplicas) {
      try {
        LOG.info("Looping until block gets rereplicated to " + numReplicas);
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      blocks = dfsClient.namenode.
                 getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }

    // Make sure the corrupt replica is invalidated and removed from
    // corruptReplicasMap
    corruptReplicaSize = cluster.getNameNode(0).namesystem.
                          corruptReplicas.numCorruptReplicas(blk);
    while (corruptReplicaSize != 0 || replicaCount != numReplicas) {
      try {
        LOG.info("Looping until corrupt replica is invalidated");
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
      }
      corruptReplicaSize = cluster.getNameNode(0).namesystem.
                            corruptReplicas.numCorruptReplicas(blk);
      blocks = dfsClient.namenode.
                 getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }
    // Make sure block is healthy 
    assertTrue(corruptReplicaSize == 0);
    assertTrue(replicaCount == numReplicas);
    assertTrue(blocks.get(0).isCorrupt() == false);
    cluster.shutdown();
  }
  
  /** Test if NameNode handles truncated blocks in block report */
  public void testTruncatedBlockReport() throws Exception {
    final Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    
    final short REPLICATION_FACTOR = (short)2;

    conf.setStrings("dfs.namenode.port.list", "0");
    conf.setLong("dfs.blockreport.intervalMsec", 30L);
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 2, true, null);
    cluster.waitActive();
   
    FileSystem fs = cluster.getFileSystem();
    try {
      final Path fileName = new Path("/file1");
      DFSTestUtil.createFile(fs, fileName, 1, REPLICATION_FACTOR, 0);
      DFSTestUtil.waitReplication(fs, fileName, REPLICATION_FACTOR);

      String block = DFSTestUtil.getFirstBlock(fs, fileName).getBlockName();

      // Truncate replica of block
      changeReplicaLength(block, 0, -1);

      
      cluster.startDataNodes(conf, 1, true, null, null);
      cluster.waitActive();  // now we have 3 datanodes
      
      // restart the datanode
      DataNodeProperties dnProps = cluster.stopDataNode(0);
      cluster.waitDatanodeDie();
      
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
      cluster.restartDataNode(dnProps);

      // wait for truncated block be detected and the block to be replicated
      DFSTestUtil.waitReplication(
          cluster.getFileSystem(), fileName, REPLICATION_FACTOR);
      
      // Make sure that truncated block will be deleted
      waitForBlockDeleted(block, 0);
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Change the length of a block at datanode dnIndex
   */
  static boolean changeReplicaLength(String blockName, int dnIndex, int lenDelta) throws IOException {
    File baseDir = new File(System.getProperty("test.build.data", "build/test/data/"), "dfs/data");
    for (int i=dnIndex*2; i<dnIndex*2+2; i++) {
      File blockFile = new File(baseDir, "data" + (i+1)+ "/current/" + 
                               blockName);
      if (blockFile.exists()) {
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        raFile.setLength(raFile.length()+lenDelta);
        raFile.close();
        return true;
      }
    }
    return false;
  }
  
  private static void waitForBlockDeleted(String blockName, int dnIndex) 
  throws IOException, InterruptedException {
    File baseDir = new File(System.getProperty("test.build.data", "build/test/data/"), "dfs/data");
    File blockFile1 = new File(baseDir, "data" + (2*dnIndex+1)+ "/current/" + 
        blockName);
    File blockFile2 = new File(baseDir, "data" + (2*dnIndex+2)+ "/current/" + 
        blockName);
    while (blockFile1.exists() || blockFile2.exists()) {
      Thread.sleep(100);
    }
  }
}
