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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.NameNodeProperties;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.mockito.internal.progress.ThreadSafeMockingProgress;

import junit.framework.TestCase;

/**
 * This class tests DatanodeDescriptor.getBlocksScheduled() at the
 * NameNode. This counter is supposed to keep track of blocks currently
 * scheduled to a datanode.
 */
public class TestMNBlocksScheduledCounter extends TestCase {
  private Configuration conf;
  static private MiniMNDFSCluster cluster;
  
  public TestMNBlocksScheduledCounter(String name) {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
  }
  
  protected void setUp() throws Exception {
    super.setUp();
    String namenodelist="0,0";    
    conf.setStrings("dfs.namenode.port.list", namenodelist);   
    int datanodeNum=1;
    cluster = new MiniMNDFSCluster(conf, datanodeNum, 0, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
  }
  
  protected void tearDown() throws Exception {
    super.tearDown();    
    if(cluster!=null){
      cluster.shutdown();
    }
    Thread.sleep(2000);
  }

  public void testBlocksScheduledCounter() throws IOException {
    FileSystem fs = cluster.getFileSystem(0);
    
    //open a file an write a few bytes:
    FSDataOutputStream out = fs.create(new Path("/testBlockScheduledCounter"), true, 4096, (short)1, 8192L);
    for (int i=0; i<1024; i++) {
      out.write(i);
    }
    // flush to make sure a block is allocated.
    ((DFSOutputStream)(out.getWrappedStream())).sync();
    
    
    ArrayList<DatanodeDescriptor> dnList = new ArrayList<DatanodeDescriptor>();
    NameNode singleNN = cluster.getNameNode(0);
    singleNN.namesystem.DFSNodesStatus(dnList, dnList);
    DatanodeDescriptor dn = dnList.get(0);
    
    assertEquals(1, dn.getBlocksScheduled());
   
    // close the file and the counter should go to zero.
    out.close();   
    assertEquals(0, dn.getBlocksScheduled());
  }
}
