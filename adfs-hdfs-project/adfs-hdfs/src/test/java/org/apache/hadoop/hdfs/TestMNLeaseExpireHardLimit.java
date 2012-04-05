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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;


/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestMNLeaseExpireHardLimit extends junit.framework.TestCase {
  static final String DIR = "/" + TestMNLeaseExpireHardLimit.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  // The test file is 2 times the blocksize plus one. This means that when the
  // entire file is written, the first two blocks definitely get flushed to
  // the datanodes.

  // creates a file but does not close it
  static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  static void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = SNAppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }
 
  /**
   * Create a file, write something, fsync but not close.
   * Then change lease period and wait for lease recovery.
   * Finally, read the block directly from each Datanode and verify the content.
   */
  public void testLeaseExpireHardLimit1() throws Exception {
    System.out.println("testLeaseExpireHardLimit1 start");
    final long leasePeriod = 3000;
    final int DATANODE_NUM = 3;
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("datanodemap.update.interval", 10000);
    conf.set("dfs.namenode.port.list","0,0");

    // create cluster
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, DATANODE_NUM, 0, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      cluster.waitDatanodeDie();
      dfs = (DistributedFileSystem)cluster.getFileSystem(0);

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestMNLeaseExpireHardLimit.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();
      int actualRepl = ((DFSClient.DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM + " datanodes but " +
          "actually " + actualRepl + "datanode(s). ", actualRepl == DATANODE_NUM);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      LocatedBlocks locations = dfs.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int successcount = 0;
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(locatedblock.getBlock().getBlockId());
        File blockfile = dataset.findBlockFile(b.getBlockId());
        System.out.println("blockfile=" + blockfile);
        if (blockfile != null) {
          BufferedReader in = new BufferedReader(new FileReader(blockfile));
          String str = in.readLine();
          System.out.println(str);
          assertEquals("something", str);
          in.close();
          successcount++;
        }
      }
      System.out.println("successcount=" + successcount);
      assertTrue(successcount > 0); 
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }

    System.out.println("testLeaseExpireHardLimit1 successful");
  }

  public void testLeaseExpireHardLimit2() throws Exception {

    System.out.println("testLeaseExpireHardLimit2 start");
    final long leasePeriod = 3000;
    final int DATANODE_NUM = 3;
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("datanodemap.update.interval", 10000);
    conf.set("dfs.namenode.port.list","0,0");

    // create cluster
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, DATANODE_NUM, 0, true, null);
    DistributedFileSystem dfs = null;
    DistributedFileSystem dfs2 = null;
    try {
      cluster.waitActive();
      cluster.waitDatanodeDie();
      dfs = (DistributedFileSystem)cluster.getFileSystem(1);
      dfs2 = (DistributedFileSystem)cluster.getFileSystem(1);

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestMNLeaseExpireHardLimit.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.sync();
      int actualRepl = ((DFSClient.DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM + " datanodes but " +
      		"actually " + actualRepl + "datanode(s). ", actualRepl == DATANODE_NUM);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      LocatedBlocks locations = dfs2.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int successcount = 0;
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(locatedblock.getBlock().getBlockId());
        File blockfile = dataset.findBlockFile(b.getBlockId());
        System.out.println("blockfile=" + blockfile);
        if (blockfile != null) {
          BufferedReader in = new BufferedReader(new FileReader(blockfile));
          String str = in.readLine();
          System.out.println(str);
          assertEquals("something", str);
          in.close();
          successcount++;
        }
      }
      System.out.println("successcount=" + successcount);
      assertTrue(successcount > 0); 
    } finally {
      if (null != dfs) IOUtils.closeStream(dfs);
      if (null != dfs2) IOUtils.closeStream(dfs2);
      if (null != cluster) cluster.shutdown();
    }

    System.out.println("testLeaseExpireHardLimit2 successful");
  }
  
  public void testLeaseExpireHardLimit3() throws Exception {
    System.out.println("testLeaseExpireHardLimit3 start");
    final long leasePeriod = 3000;
    final int DATANODE_NUM = 5;
    Configuration conf = new Configuration();
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setInt("datanodemap.update.interval", 10000);
    conf.set("dfs.namenode.port.list","0,0");

    // create cluster
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, DATANODE_NUM, 0, true, null);
    cluster.startDataNodes(conf, DATANODE_NUM, 1, true, null, null, null);
    DistributedFileSystem dfs = null;
    DistributedFileSystem dfs2 = null;
    try {
      cluster.waitActive();
      cluster.waitDatanodeDie();
      dfs = (DistributedFileSystem)cluster.getFileSystem(0);
      dfs2 = (DistributedFileSystem)cluster.getFileSystem(1);

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestMNLeaseExpireHardLimit.createFile(dfs, fpath, DATANODE_NUM*2);
      out.write("something".getBytes());
      out.sync();
      int actualRepl = ((DFSClient.DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM*2 + " datanodes but " +
          "actually " + actualRepl + "datanode(s). ", actualRepl == DATANODE_NUM*2);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      LocatedBlocks locations = dfs2.dfs.namenode.getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int successcount = 0;
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(locatedblock.getBlock().getBlockId());
        File blockfile = dataset.findBlockFile(b.getBlockId());
        System.out.println("blockfile=" + blockfile);
        if (blockfile != null) {
          BufferedReader in = new BufferedReader(new FileReader(blockfile));
          String str = in.readLine();
          System.out.println(str);
          assertEquals("something", str);
          in.close();
          successcount++;
        }
      }
      System.out.println("successcount=" + successcount);
      assertTrue(successcount > 0); 
    } finally {
      if (null != dfs) IOUtils.closeStream(dfs);
      if (null != dfs2) IOUtils.closeStream(dfs2);
      if (null != cluster) cluster.shutdown();
    }

    System.out.println("testLeaseExpireHardLimit3 successful");
  }
}
