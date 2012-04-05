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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;

/**
 * This class tests the cases of a concurrent reads/writes to a file;
 * ie, one writer and one or more readers can see unfinsihed blocks
 */
public class TestMNDataBlockScanner extends junit.framework.TestCase {
  private static final Logger LOG = 
    Logger.getLogger(TestSNDataBlockScanner.class);
  
  {
    ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int blockSize = 8192;
  private MiniMNDFSCluster cluster;
  private FileSystem fileSystem;

  
  @Before
  protected void setUp() throws Exception {
    super.setUp();
    final Configuration conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list", "0,0,0");
    init(conf);    
  }

  @Override
  protected void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  private void init(Configuration conf) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
    // start up one datanode and connects it to NN 0
    cluster = new MiniMNDFSCluster(conf, 1, 0, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    //fileSystem = cluster.getFileSystem(0);
  }

  /**
   * client writes file from one NN and read from another NN, 
   * both different from the NN of datanode. 
   * @throws IOException
   * @throws InterruptedException
   */
  public void testPrematureDataBlockScannerAddDiffNN() 
      throws IOException, InterruptedException {
    fileSystem = cluster.getFileSystem(0);
    // check that / exists
    Path path = new Path("/");
    System.out.println("Path : \"" + path.toString() + "\"");
    System.out.println(fileSystem.getFileStatus(path).isDir());
    assertTrue("/ should be a directory", fileSystem.getFileStatus(path)
        .isDir());
    
    writeAndRead(fileSystem, cluster.getFileSystem(1));
  }
  
  /**
   * client writes a file and reads it using the same NN, 
   * but different from the NN of datanode. 
   * @throws IOException 
   */
  public void testPrematureDataBlockScannerAddSameNN() throws IOException{
    writeAndRead(cluster.getFileSystem(2), cluster.getFileSystem(1));
  }
  
  private void writeAndRead(FileSystem fsWrite, FileSystem fsRead)
      throws IOException {
    // create a new file in the root, write data, do no close
    Path file1 = new Path("/unfinished-block");
    FSDataOutputStream out = fsWrite.create(file1);
    
    int writeSize = blockSize / 2;
    out.write(new byte[writeSize]);
    out.sync();
    
    FSDataInputStream in = fsRead.open(file1);
    
    byte[] buf = new byte[4096];
    in.readFully(0, buf);
    in.close();
    
    waitForBlocks(fsWrite, file1, 1, writeSize);
    
    int blockMapSize = cluster.getDataNodes().get(0).blockScanner.blockMap
        .size();
    assertTrue(String.format("%d entries in blockMap and it should be empty",
        blockMapSize), blockMapSize == 0);
    
    out.close();
  }
  
private void waitForBlocks(FileSystem fileSys, 
		                   Path name, 
		                   int blockCount, 
		                   long length)
    throws IOException {
    // wait until we have at least one block in the file to read.
    boolean done = false;

    while (!done) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      done = true;
      BlockLocation[] locations = fileSys.getFileBlockLocations(
        fileSys.getFileStatus(name), 0, length);
      if (locations.length < blockCount) {
        done = false;
        continue;
      }
    }
  }
  
}
