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
import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.StringUtils;

public class TestMNAbandonBlock extends junit.framework.TestCase {
  public static final Log LOG = LogFactory.getLog(TestMNAbandonBlock.class);
  
  static final String FILE_NAME_PREFIX
      = "/" + TestMNAbandonBlock.class.getSimpleName() + "_"; 
  
  final static private Configuration conf = new Configuration();
  static private MiniMNDFSCluster cluster;
  
  public TestMNAbandonBlock(String name) {
    super(name);
    URL url=DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
  }
  
  protected void setUp() throws Exception {
    super.setUp();
    String namenodelist="0,0";
    int datanodeNum=5;
    conf.setStrings("dfs.namenode.port.list", namenodelist);
    cluster = new MiniMNDFSCluster(conf, datanodeNum, true, null);
    cluster.waitDatanodeDie();
  }

  protected void tearDown() throws Exception {
    super.tearDown();    
    cluster.shutdown();
    Thread.sleep(2000);
  }
  
  public void testMNAbandonBlockSameNN() throws Exception {
    doAbandonBlock(true);
  }
  
  public void testMNAbandonBLockDiffNN() throws Exception {
    doAbandonBlock(false);
  }

  public void doAbandonBlock(boolean sameNN) throws IOException {
    FileSystem fs = cluster.getFileSystem(0);
    String src = FILE_NAME_PREFIX + "foo_" + System.nanoTime();
    FSDataOutputStream fout = null;
    try {
      //start writing a a file but not close it
      fout = fs.create(new Path(src), true, 4096, (short)1, 512L);
      for(int i = 0; i < 1024; i++) {
        fout.write(123);
      }
      fout.sync();
  
      //try reading the block by someone
      InetSocketAddress nnAddr =null;
      if( sameNN ){
        nnAddr=NameNode.getAddress("localhost:"+cluster.getNameNodePort(0));
      } else {
        nnAddr=NameNode.getAddress("localhost:"+cluster.getNameNodePort(1));
      }
      
//      final DFSClient dfsclient = new DFSClient(NameNode.getAddress(conf), conf);
      final DFSClient dfsclient = new DFSClient(nnAddr, conf);
      LocatedBlocks blocks = dfsclient.namenode.getBlockLocations(src, 0, 1);
      LocatedBlock b = blocks.get(0); 
      try {
          dfsclient.namenode.abandonBlock(b.getBlock(), src, "someone");
          //previous line should throw an exception.
          assertTrue(false);

      }
      catch(IOException ioe) {
        LOG.info("GREAT! " + StringUtils.stringifyException(ioe));
      }
    }
    finally {
      try{fout.close();} catch(Exception e) {}
      try{fs.close();} catch(Exception e) {}
    }
  }
}
