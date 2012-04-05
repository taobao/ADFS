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
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

/**
 * This class tests the replication of a DFS file.
 */
public class TestMNReplication extends TestCase {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 4096;
  private static final int fileSize = blockSize*2;
  private static final String racks[] = new String[] {
    "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3"
  };
  private static final int numDatanodes = 7; //racks.length;
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hdfs.TestReplication");

  private void writeFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    DFSTestUtil.waitReplication(fileSys, name, (short)(Math.min(repl, numDatanodes)));
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /* 
   * Test if Datanode reports bad blocks during replication request
   */
  public void testBadBlockReportOnTransfer() throws Exception {
    Configuration conf = new Configuration();
    conf.setStrings("dfs.namenode.port.list", "0,0");
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 2, 1, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    fs = cluster.getFileSystem(0);
    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                              cluster.getNameNodePort(0)), conf);
  
    // Create file with replication factor of 1
    Path file1 = new Path("/tmp/testBadBlockReportOnTransfer/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)1, 0);
    DFSTestUtil.waitReplication(fs, file1, (short)1);
  
    // Corrupt the block belonging to the created file
    String block = DFSTestUtil.getFirstBlock(fs, file1).getBlockName();
    cluster.corruptBlockOnDataNodes(block);
  
    // Increase replication factor, this should invoke transfer request
    // Receiving datanode fails on checksum and reports it to namenode
    fs.setReplication(file1, (short)2);
  
    // Now get block details and check if the block is corrupt
    blocks = dfsClient.namenode.
              getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    while (blocks.get(0).isCorrupt() != true) {
      try {
        LOG.info("Waiting until block is marked as corrupt...");
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
      }
      blocks = dfsClient.namenode.
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    }
    replicaCount = blocks.get(0).getLocations().length;
    assertTrue(replicaCount == 1);
    cluster.shutdown();
  }
  
  /**
   * Tests replication in DFS.
   */
  public void runReplication(boolean simulated) throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.replication.considerLoad", false);
    if (simulated) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    conf.setStrings("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, numDatanodes, 1, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    DatanodeInfo[] info = cluster.getDataNodeReportType(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem(0);
    try {
      Path file1 = new Path("/smallblocktest.dat");
      writeFile(fileSys, file1, 3);
      printLog(cluster,simulated);
      checkFile(fileSys, file1, 3);
      cleanupFile(fileSys, file1);
      cluster.waitDatanodeDie();
      printLog(cluster,simulated);
      writeFile(fileSys, file1, 10);
      printLog(cluster,simulated);
      checkFile(fileSys, file1, 10);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 4);
      printLog(cluster,simulated);
      checkFile(fileSys, file1, 4);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 1);
      checkFile(fileSys, file1, 1);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 2);
      checkFile(fileSys, file1, 2);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }


  private void printLog(MiniMNDFSCluster cluster, boolean simulated) {
	  List<DataNode> datanodes = cluster.getDataNodes();
	  for(DataNode node: datanodes)
	  {
		  FSDatasetInterface data = node.data;
		  Block[] blocks = data.getBlockReport();
		  System.out.println("++storageId" + data.getStorageInfo());
		  for(int i=0;i<blocks.length;i++)
			  System.out.println("blockid " + blocks[i].getBlockId() +", ");
	  }
}

public void testReplicationSimulatedStorag() throws IOException {
    runReplication(true);
  }
  
  
  public void testReplication() throws IOException {
    runReplication(false);
  }
  
  /* This test makes sure that NameNode retries all the available blocks 
   * for under replicated blocks. 
   * 
   * It creates a file with one block and replication of 4. It corrupts 
   * two of the blocks and removes one of the replicas. Expected behaviour is
   * that missing replica will be copied from one valid source.
   */
  public void testPendingReplicationRetry() throws IOException {
    
    MiniMNDFSCluster cluster = null;
    int numDataNodes = 4;
    String testFile = "/replication-test-file";
    Path testPath = new Path(testFile);
    
    byte buffer[] = new byte[1024];
    for (int i=0; i<buffer.length; i++) {
      buffer[i] = '1';
    }
    
    try {
      Configuration conf = new Configuration();
      conf.setStrings("dfs.namenode.port.list", "0,0");
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      //first time format
      cluster = new MiniMNDFSCluster(0, conf, numDataNodes, true,
                                   true, null, null);
      cluster.waitActive();
      cluster.waitDatanodeDie();
//      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
//                                            cluster.getNameNodePort(0)),
//                                            conf);
      FileSystem fileSys = cluster.getFileSystem(0);
      OutputStream out = fileSys.create(testPath, true,
              fileSys.getConf().getInt("io.file.buffer.size", 4096),
              (short)numDataNodes, (long)blockSize);
      out.write(buffer);
      out.close();
      
//      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);
      DFSTestUtil.waitReplication(fileSys, testPath, (short)numDataNodes);

      // get first block of the file.
//      String block = dfsClient.namenode.
//                       getBlockLocations(testFile, 0, Long.MAX_VALUE).
//                       get(0).getBlock().getBlockName();
      
      String block = cluster.getNameNode(0).getBlockLocations(testFile, 0, Long.MAX_VALUE).
    		  			get(0).getBlock().getBlockName();
      
      cluster.shutdown(false);
      cluster = null;
      
      //Now mess up some of the replicas.
      //Delete the first and corrupt the next two.
      File baseDir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/data");
      for (int i=0; i<25; i++) {
        buffer[i] = '0';
      }
      
      int fileCount = 0;
      for (int i=0; i<6; i++) {
        File blockFile = new File(baseDir, "data" + (i+1) + "/current/" + block);
        LOG.info("Checking for file " + blockFile);
        
        if (blockFile.exists()) {
          if (fileCount == 0) {
            LOG.info("Deleting file " + blockFile);
            assertTrue(blockFile.delete());
          } else {
            // corrupt it.
            LOG.info("Corrupting file " + blockFile);
            long len = blockFile.length();
            assertTrue(len > 50);
            RandomAccessFile blockOut = new RandomAccessFile(blockFile, "rw");
            try {
              blockOut.seek(len/3);
              blockOut.write(buffer, 0, 25);
            } finally {
              blockOut.close();
            }
          }
          fileCount++;
        }
      }
      assertEquals(3, fileCount);
      
      /* Start the MiniDFSCluster with more datanodes since once a writeBlock
       * to a datanode node fails, same block can not be written to it
       * immediately. In our case some replication attempts will fail.
       */
      
      LOG.info("Restarting minicluster after deleting a replica and corrupting 2 crcs");
      conf = new Configuration();
      conf.set("dfs.replication", Integer.toString(numDataNodes));
      conf.set("dfs.replication.pending.timeout.sec", Integer.toString(2));
      conf.set("dfs.datanode.block.write.timeout.sec", Integer.toString(5));
      conf.set("dfs.safemode.threshold.pct", "0.75f"); // only 3 copies exist
      conf.setStrings("dfs.namenode.port.list", "0,0");

      cluster = new MiniMNDFSCluster(conf, numDataNodes, 1, false, null);
      cluster.startDataNodes(conf, numDataNodes, 1, true, null, null, null);
      cluster.waitActive();
      
//      dfsClient = new DFSClient(new InetSocketAddress("localhost",
//                                  cluster.getNameNodePort(0)),
//                                  conf);
//      
//      waitForBlockReplication(testFile, dfsClient.namenode, numDataNodes, -1);
      fileSys = cluster.getFileSystem(0);
      DFSTestUtil.waitReplication(fileSys, testPath, (short)numDataNodes);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }  
  }
  
  /**
   * Test if replication can detect mismatched length on-disk blocks
   * @throws Exception
   */
  public void testReplicateLenMismatchedBlock() throws Exception {
    Configuration conf =  new Configuration();
    conf.setStrings("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 2, 1, true, null);
    try {
      cluster.waitActive();
      cluster.waitDatanodeDie();
      // test truncated block
      changeBlockLen(cluster, -1);
      // test extended block
      changeBlockLen(cluster, 1);
    } finally {
      cluster.shutdown();
    }
  }
  
  private void changeBlockLen(MiniMNDFSCluster cluster, 
      int lenDelta) throws IOException, InterruptedException {
    final Path fileName = new Path("/file1");
    final short REPLICATION_FACTOR = (short)1;
    final FileSystem fs = cluster.getFileSystem(0);
    final int fileLen = fs.getConf().getInt("io.bytes.per.checksum", 512);
    DFSTestUtil.createFile(fs, fileName, fileLen, REPLICATION_FACTOR, 0);
    DFSTestUtil.waitReplication(fs, fileName, REPLICATION_FACTOR);

    String block = DFSTestUtil.getFirstBlock(fs, fileName).getBlockName();

    // Change the length of a replica
    for (int i=0; i<cluster.getDataNodes().size(); i++) {
      if (TestSNDatanodeBlockScanner.changeReplicaLength(block, i, lenDelta)) {
        break;
      }
    }

    // increase the file's replication factor
    fs.setReplication(fileName, (short)(REPLICATION_FACTOR+1));

    // block replication triggers corrupt block detection
    DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost", 
        cluster.getNameNodePort(0)), fs.getConf());
    LocatedBlocks blocks = dfsClient.namenode.getBlockLocations(
        fileName.toString(), 0, fileLen);
    if (lenDelta < 0) { // replica truncated
    	while (!blocks.get(0).isCorrupt() || 
    			REPLICATION_FACTOR != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.namenode.getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    } else { // no corruption detected; block replicated
    	while (REPLICATION_FACTOR+1 != blocks.get(0).getLocations().length) {
    		Thread.sleep(100);
    		blocks = dfsClient.namenode.getBlockLocations(
    				fileName.toString(), 0, fileLen);
    	}
    }
    fs.delete(fileName, true);
  }
}
