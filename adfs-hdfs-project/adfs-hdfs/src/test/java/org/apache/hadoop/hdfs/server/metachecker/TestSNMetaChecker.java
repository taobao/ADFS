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

 package org.apache.hadoop.hdfs.server.metachecker;

import java.net.URL;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.Stat;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.state.StateManager;
import com.taobao.adfs.util.IpAddress;

import junit.framework.TestCase;

/**
 * This testcase is the SN version for MetaChecker. See detail at 
 * http://110.75.12.5:9999/browse/NNR-550 .
 */
public class TestSNMetaChecker extends TestCase {
  private Configuration conf;
  public static final Log LOG = LogFactory.getLog(TestSNMetaChecker.class);
  private StateManager state;
  private long datanodeExpireInterval; 

  public TestSNMetaChecker(String name) throws Exception {
    super(name);
    conf = new Configuration();
    URL url=DFSTestUtil.class.getResource("/mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list", "0");
    state = new StateManager(conf);
  }

  protected void setUp() throws Exception {
    super.setUp();
    ZKClient.getInstance(conf).delete(FSConstants.ZOOKEEPER_METACHECKER_HOME+"/token", false, true);
    ZKClient.getInstance(conf).delete(FSConstants.ZOOKEEPER_METACHECKER_HOME, false, true);
    
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000; 
    long datanodeMapUpdateInterval = conf.getInt("datanodemap.update.interval", 5 * 60 * 1000);
    datanodeExpireInterval = 2 * datanodeMapUpdateInterval + 10 * heartbeatInterval;
  }

  protected void tearDown() throws Exception {
    super.tearDown();    
  }
  
  /*
   * does junit test support multiple thread?
   * to do
  public void testSingleton() throws Exception{  
    int status=ToolRunner.run(conf, new BlockChecker(), null);
    assertEquals("run blockChecker failed",BlockChecker.SUCCESS,status);
    status=ToolRunner.run(conf, new BlockChecker(), null);
    assertEquals("block checker run in duplicate,should be singleton",
                 BlockChecker.ALREADY_RUNNING,status);
  }
  */
  
  public void testParameter() throws Exception{
    ToolRunner.run(conf, new MetaChecker(), null);
    Stat stat=null;
    while(true){
      stat=ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_METACHECKER_HOME+"/token");
      if(stat==null){
        break;
      }
      TimeUnit.SECONDS.sleep(2);
    }
    int status=ToolRunner.run(conf, new MetaChecker(), null);
    assertEquals("block checker runtime less then interval,should not run",
                 MetaChecker.SHORT_TIME,status);
    String[] args={"-force"};
    status=ToolRunner.run(conf, new MetaChecker(), args);
    assertEquals("block checker run in force mode,but failed",
                MetaChecker.SUCCESS,status);
  }
  
  public void testAddRepl() throws Exception{    
    Random r = new Random();
    MiniMNDFSCluster cluster=new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    
    FileSystem fs=cluster.getFileSystem();
    Path fileName = new Path("/blockchecker_toAdd");
    fs.deleteOnExit(fileName);
    DFSTestUtil.createFile(fs, fileName,
                           1024, (short)3, r.nextLong());
    DFSTestUtil.waitReplication(fs,fileName, (short)3);
    
    int rawID=state.getFileInfo("/"+fileName.getName()).id;
    List<BlockEntry> beList=state.getBlocksByFileID(rawID);
    List<Integer> datanodes=beList.get(0).getDatanodeIds();
    int random=r.nextInt(datanodes.size());
    state.removeBlockReplicationOnDatanodeBy(datanodes.get(random), beList.get(0).getBlockId());
    assertEquals(3,state.getFileInfo(rawID).replication);
    
    beList=state.getBlocksByFileID(rawID);
    assertEquals(2,beList.get(0).getDatanodeIds().size());
    
    ToolRunner.run(conf, new MetaChecker(), null);
    assertEquals(3,state.getFileInfo(rawID).replication);
    
    beList=state.getBlocksByFileID(rawID);
    assertEquals(3,beList.get(0).getDatanodeIds().size());
    fs.deleteOnExit(fileName);    
    
    fs.close();
    cluster.shutdown();
    Thread.sleep(60000);
  }
  
  public void testReduceRepl() throws Exception{
    Random r = new Random();
    MiniMNDFSCluster cluster=new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    
    FileSystem fs=cluster.getFileSystem();
    Path fileName = new Path("/blockchecker_toReduce");
    fs.deleteOnExit(fileName);
    DFSTestUtil.createFile(fs, fileName,
                           1024, (short)3, r.nextLong());
    DFSTestUtil.waitReplication(fs,fileName, (short)3);
    try {
      Thread.sleep(60000);
    } catch (Exception e) {}
    
    int rawID=state.getFileInfo("/"+fileName.getName()).id;
    List<BlockEntry> beList=state.getBlocksByFileID(rawID);
    state.setReplication("/"+fileName.getName(), (byte)2);
    assertEquals(2,state.getFileInfo(rawID).replication);
    assertEquals(3,beList.get(0).getDatanodeIds().size());
    
    ToolRunner.run(conf, new MetaChecker(), null);
    assertEquals(2,state.getFileInfo(rawID).replication);
    beList=state.getBlocksByFileID(rawID);
    assertEquals(2,beList.get(0).getDatanodeIds().size());
    fs.deleteOnExit(fileName);
    
    fs.close();
    cluster.shutdown();
    Thread.sleep(60000);
  }
  
  public void testUnderDelete() throws Exception{
    LOG.info("Run into testUnderDelete");
    Random r = new Random();
    MiniMNDFSCluster cluster=new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    
    FileSystem fs=cluster.getFileSystem();
    String strPath = "/blockchecker_toReduce";
    Path fileName = new Path(strPath);
    fs.deleteOnExit(fileName);
    DFSTestUtil.createFile(fs, fileName,
                           1024, (short)3, r.nextLong());
    DFSTestUtil.waitReplication(fs,fileName, (short)3);
    LOG.info("We will set rep to 2");
    fs.setReplication(fileName, (short)2);
    int rawID = state.getFileInfo(strPath).id;
    List<BlockEntry> beList = state.getBlocksByFileID(rawID);
    LOG.info("We will block checker");
    ToolRunner.run(conf, new MetaChecker(), null);
    assertEquals(2,state.getFileInfo(rawID).replication);
    assertEquals(3,beList.get(0).getDatanodeIds().size());
    fs.deleteOnExit(fileName);
    LOG.info("We will close fs and file");
    fs.close();
    cluster.shutdown();
    Thread.sleep(60000);
  }
  
  public void testZookeeperException() throws Exception{
    String invalidatePath=FSConstants.ZOOKEEPER_INVALIDATE_HOME+"/test";
    String excessPath=FSConstants.ZOOKEEPER_EXCESS_HOME+"/test";
    String corruptPath=FSConstants.ZOOKEEPER_CORRUPT_HOME+"/test";
    ZKClient.getInstance().delete(invalidatePath, false, true);
    ZKClient.getInstance().delete(excessPath, false, true);
    ZKClient.getInstance().delete(corruptPath, false, true);
    ZKClient.getInstance().create(invalidatePath, null, true, true);
    ToolRunner.run(conf, new MetaChecker(), null);
    Stat stat=ZKClient.getInstance().exist(invalidatePath);
    assertNull(stat);
    stat=ZKClient.getInstance().exist(excessPath);
    assertNull(stat);
    stat=ZKClient.getInstance().exist(corruptPath);
    assertNull(stat);
    ZKClient.getInstance().delete(invalidatePath, false, true);
    ZKClient.getInstance().delete(excessPath, false, true);
    ZKClient.getInstance().delete(corruptPath, false, true);
  }
  
  /**
   * Namenode may fail to delete all blocks after it remove a datanode.
   * Here we check this possibility. 
   * <p>
   * In this testcase, we first start the cluster, insert a faked DN into DB, 
   * and insert a faked block into DB using the faked datanode. After running 
   * MetaChecker, this faked record should disappear. 
   */
  public void testDeadBlockRepFromDeadDN() throws Exception {
    MiniMNDFSCluster cluster=new MiniMNDFSCluster(conf, 1, true, null);
    try {
      cluster.waitActive();
      DatanodeRegistration dnReg = new DatanodeRegistration("196.168.0.100:12000");
      dnReg.setStorageID("DS-519212136-196.168.0.100-12000-1325226701074");
      int dnID = IpAddress.getAddress(dnReg.getHost());
      // To make sure the faked datanode appears dead, use an old update time. 
      state.registerDatanodeBy(dnReg, "192.168.0.100", 
                               System.currentTimeMillis() - 10*datanodeExpireInterval);
      // add the faked block.
      long fakedBlkID = 12000L;
      long fakedGenStamp = 1001;
      Block b = new Block(fakedBlkID, 0, 0);
      state.addBlockToFileBy(b, 10001, 0);
      state.receiveBlockFromDatanodeBy(dnID, fakedBlkID, 20, fakedGenStamp);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {}
      ToolRunner.run(conf, new MetaChecker(), null);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {}
      BlockEntry be = state.getStoredBlockBy(fakedBlkID, fakedGenStamp);
      assertTrue(be == null || !(be.getDatanodeIds().contains(new Integer(dnID))));
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * It is possible that a file is deleted but left some blocks in DB.
   * <p>
   * This testcase starts a cluster, and insert a faked block into DB. After 
   * running MetaChecker, this faked block should disappear. 
   * @throws Exception
   */
  public void testDeadBlockFromDeadFile() throws Exception {
    MiniMNDFSCluster cluster=new MiniMNDFSCluster(conf, 3, true, null);
    try {
      cluster.waitActive();
      DatanodeRegistration dnReg = new DatanodeRegistration("192.168.0.0:"+
                        (cluster.getDataNodes().get(0)).getSelfAddr().getPort());
      dnReg.setStorageID("DS-519212136-196.168.0.100-12000-1325226701074");
      int dnID = IpAddress.getAddress(dnReg.getHost());
      // add the faked block.
      long fakedBlkID = 12000L;
      long fakedGenStamp = 1001;
      Block b = new Block(fakedBlkID, 0, 0);
      state.addBlockToFileBy(b, 10001, 0);
      state.receiveBlockFromDatanodeBy(dnID, fakedBlkID, 20, fakedGenStamp);
      
      ToolRunner.run(conf, new MetaChecker(), null);
      BlockEntry be = state.getStoredBlockBy(fakedBlkID, fakedGenStamp);
      assertTrue(be == null || !(be.getDatanodeIds().contains(new Integer(dnID))));
    } finally {
      cluster.shutdown();
    }
  }
}
