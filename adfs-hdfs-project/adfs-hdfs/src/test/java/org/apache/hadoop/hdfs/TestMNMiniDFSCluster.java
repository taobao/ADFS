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
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.MiniMNDFSCluster.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.eclipse.jdt.core.dom.ThisExpression;

import junit.framework.TestCase;

/**
 * This test ensures MiniMNDFSCluster can build all simulation case
 */
public class TestMNMiniDFSCluster extends TestCase {
  
  final static private Configuration conf = new Configuration();
  final static private int NUM_OF_DATANODES = 4;  // must be a event number
 
  MiniMNDFSCluster cluster = null;
  
  @Override
  protected void setUp() throws Exception {
    // TODO Auto-generated method stub
    super.setUp();
    conf.setInt("heartbeat.recheck.interval", 500); // 0.5s
    conf.setInt("datanodemap.update.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setStrings("dfs.namenode.port.list", "0,0");
    cluster = new MiniMNDFSCluster(conf, NUM_OF_DATANODES, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
  }

  public int getNamenodeNum() {
    return this.cluster.getNameNodes().size();
  }

  public NameNodeProperties stopNamenode(int index) {
    assert index < this.cluster.getNameNodes().size() && index >= 0 : "namenode index overflow";
    return this.cluster.stopNameNode(index);
  }
  
  public NameNodeProperties startNamenode(NameNodeProperties nnprop) throws Exception {
    return this.cluster.startNewNameNode(nnprop.conf, nnprop.namenode.getNameNodeAddress().getPort(), null);
  }
  
  public DataNodeProperties stopDatanode(int index) {
    assert index < this.cluster.getDataNodes().size() && index >= 0 : "datanode index overflow";
    return this.cluster.stopDataNode(index);
  }
  
  public DataNodeProperties startDatanode(DataNodeProperties dnprop) throws Exception {
    this.cluster.startDataNodes(dnprop.conf, 1, true, null, null);
    return null;
  }
  
  public void testMiniMNDFSCluster() throws Exception {
    try{
      Random r = new Random();
      
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.ALL).length);
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.LIVE).length);
      assertEquals(0, this.cluster.getDataNodeReportType(DatanodeReportType.DEAD).length);
      
      int numNamenode = getNamenodeNum();
      NameNodeProperties  nnprop = this.stopNamenode(numNamenode - 1);
//      Thread.sleep(1000*conf.getInt("dfs.namenode.selector.maxretrycount", 60));
      cluster.waitDatanodeDie();
      waitForDataNode(cluster, NUM_OF_DATANODES);
      //the max handshake time of datanode is 5, and every handshake has 9 connections most  
      Thread.sleep(NUM_OF_DATANODES*5*9*1000);
      
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.ALL).length);
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.LIVE).length);
      assertEquals(0, this.cluster.getDataNodeReportType(DatanodeReportType.DEAD).length);
      
      DataNodeProperties dnprop = this.stopDatanode(NUM_OF_DATANODES - 1);

      cluster.waitDatanodeDie();
//      waitForDataNode(cluster, NUM_OF_DATANODES-1);
//      assertEquals(NUM_OF_DATANODES - 1, this.cluster.getDataNodeReportType(DatanodeReportType.ALL).length);
      assertEquals(NUM_OF_DATANODES - 1, this.cluster.getDataNodeReportType(DatanodeReportType.LIVE).length);
      
      this.startNamenode(nnprop);
      this.startDatanode(dnprop);
//      cluster.waitDatanodeDie();
      
      waitForDataNode(cluster, NUM_OF_DATANODES);
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.ALL).length);
      assertEquals(NUM_OF_DATANODES, this.cluster.getDataNodeReportType(DatanodeReportType.LIVE).length);
      assertEquals(0, this.cluster.getDataNodeReportType(DatanodeReportType.DEAD).length);
      
    } finally {
      this.cluster.shutdown();
    }
  }
  
  private void waitForDataNode(MiniMNDFSCluster cluster, int numOfDatanodes) throws IOException, InterruptedException {
	  int numOfDN=0;
      long start = System.currentTimeMillis();
      while((numOfDN =cluster.getDataNodeReportType(DatanodeReportType.ALL).length)!=numOfDatanodes)
      {
    	  Thread.sleep(1000*3);
    	  System.out.println("current dn is " + numOfDN + "++++++++++++++++++++++++++++++++excpte is " + numOfDatanodes);
      }
      long end = System.currentTimeMillis();
      System.out.println("total time is " + (end-start)/1000.0 + "++++++++++++++++++++++++++++++++");
	
}

/**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    new TestMNMiniDFSCluster().testMiniMNDFSCluster();
  }
}
