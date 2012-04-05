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

package com.taobao.adfs.state;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.block.BlockInternalProtocol;
import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.datanode.ExDatanodeInfo;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepositoryTest;
import com.taobao.adfs.state.StateManager;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerTest {
  protected static StateManager stateManager;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
    Configuration conf = new Configuration(false);
    conf.set("distributed.client.enabled", "false");
    conf.set("distributed.data.path", "target/test" + StateManagerTest.class.getSimpleName());
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf.setInt("file.cache.capacity", 1);
    conf.set("distributed.data.format", "true");
    stateManager = new StateManager(conf);
  }

  @Before
  public void setupBeforeTest() throws Exception {
    stateManager.format();
  }

  @Test
  public void format() throws Exception {
    stateManager.mkdirs("/a");
    stateManager.addBlockToFileBy(new org.apache.hadoop.hdfs.protocol.Block(0L, 0L, 0L), 1, 0);
    stateManager.registerDatanodeBy(new DatanodeRegistration(), "www.tbdfs.com", System.currentTimeMillis());
    stateManager.format();
    assertThat(stateManager.getListing("/").length == 0, is(true));
    assertThat(stateManager.getAllBlocksOnDatanode(0).isEmpty(), is(true));
    assertThat(stateManager.getAliveDatanodes(Long.MAX_VALUE).isEmpty(), is(true));
  }

  @Test
  public void getFileInfo() throws Exception {
    File file = stateManager.getFileInfo("/");
    assertThat(file.isDir(), is(true));
    assertThat(file.path, is("/"));
    assertThat(file.replication, is((byte) 0));
    assertThat(file.length, is(-1L));
    assertThat(file.blockSize, is(0));
    if (getClass() != StateManagerBaseOnDistributedClientTest.class) assertThat(file.getOperateIdentifier() == null,
        is(true));
    else assertThat(file.getOperateIdentifier().startsWith("127.0.0.1-"), is(true));

    String path = "/file";
    assertThat(stateManager.getFileInfo(path), is(nullValue()));

    // make sure getFileInfo by a not existed id will not throw an exception
    while (stateManager.getFileInfo(file.id++) != null);

    file = stateManager.create(path, false, (byte) 5, 1024);
    assertThat(file, is(stateManager.getFileInfo(path)));
    assertThat(file.isDir(), is(false));
    assertThat(file.path, is(path));
    assertThat(file.replication, is((byte) 5));
    assertThat(file.blockSize, is(1024));
    assertThat(file.length, is(0L));
    assertThat(file, is(stateManager.getFileInfo(file.id)));

    stateManager.complete(file.id, 1024);
    file = stateManager.getFileInfo(path);
    assertThat(file.isDir(), is(false));
    assertThat(file.path, is(path));
    assertThat(file.replication, is((byte) 5));
    assertThat(file.blockSize, is(1024));
    assertThat(file.length, is(1024L));
    assertThat(file, is(stateManager.getFileInfo(file.id)));

    path = "/dir";
    stateManager.mkdirs(path);
    file = stateManager.getFileInfo(path);
    assertThat(file.isDir(), is(true));
    assertThat(file.path, is(path));
    assertThat(file.replication, is((byte) 0));
    assertThat(file.blockSize, is(0));
    assertThat(file.length, is(-1L));
    assertThat(file, is(stateManager.getFileInfo(file.id)));
  }

  @Test
  public void getListing() throws Exception {
    assertThat(stateManager.getListing("/").length, is(0));
    assertThat(stateManager.getListing("/notExistedDir"), is(nullValue()));

    File file = stateManager.create("/file", false, (byte) 3, 1024);
    stateManager.complete(file.id, 1024);
    assertThat(stateManager.getListing("/").length, is(1));
    assertThat(stateManager.getListing("/"), hasItemInArray(stateManager.getFileInfo("/file")));

    stateManager.mkdirs("/dir");
    assertThat(stateManager.getListing("/").length, is(2));
    assertThat(stateManager.getListing("/"), hasItemInArray(stateManager.getFileInfo("/dir")));
  }

  @Test
  public void getDescendant() throws Exception {
    stateManager.create("/a/b/c/d", true, (byte) 0, 0);
    stateManager.create("/A", true, (byte) 0, 0);

    assertThat(stateManager.getDescendant("/", false, false).length == 6, is(true));
    assertThat(stateManager.getDescendant("/", true, false).length == 2, is(true));
    assertThat(stateManager.getDescendant("/a", false, false).length == 4, is(true));
    assertThat(stateManager.getDescendant("/a", true, false).length == 1, is(true));
    assertThat(stateManager.getDescendant(stateManager.getFileInfo("/"), false, false).length == 6, is(true));
    assertThat(stateManager.getDescendant(stateManager.getFileInfo("/"), true, false).length == 2, is(true));

    assertThat(stateManager.getDescendant("/", true, true).length == 3, is(true));
    assertThat(stateManager.getDescendant(stateManager.getFileInfo("/"), true, true).length == 3, is(true));
  }

  @Test
  public void deleteFile() throws Exception {
    assertThat(stateManager.create("/file", true, (byte) 0, 0) != null, is(true));
    assertThat(stateManager.delete("/file", false).length, is(1));
    assertThat(stateManager.getListing("/").length, is(0));

    assertThat(stateManager.mkdirs("/emptyDir"), is(true));
    assertThat(stateManager.delete("/emptyDir", false).length, is(0));
    assertThat(stateManager.getListing("/").length, is(0));

    stateManager.create("/notEmptyDir/file", true, (byte) 0, 0);
    stateManager.mkdirs("/notEmptyDir/dir");
    try {
      stateManager.delete("/notEmptyDir", false);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
    File[] deletedFiles = stateManager.delete("/notEmptyDir");
    assertThat(deletedFiles.length, is(1));
    assertThat(deletedFiles[0].path, is("/notEmptyDir/file"));
    assertThat(stateManager.getListing("/").length, is(0));
  }

  @Test
  public void deleteFileAndInsertAgain() throws Exception {
    // test cooperation of delete and insert
    assertThat(stateManager.mkdirs("/0/1/2/3/4/5/6/7/8/9"), is(true));
    assertThat(stateManager.delete("/0", true).length, is(0));
    assertThat(stateManager.mkdirs("/0/1/2/3/4/5/6/7/8/9"), is(true));
  }

  @Test
  public void deleteFileByFile() throws Exception {
    assertThat(stateManager.mkdirs("/dir1/1/a"), is(true));
    assertThat(stateManager.mkdirs("/dir1/1/b"), is(true));
    assertThat(stateManager.mkdirs("/dir1/2/c"), is(true));
    assertThat(stateManager.getDescendant("/", false, false).length, is(7));
    try {
      stateManager.delete(stateManager.getFileInfo("/dir1"), false);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
    assertThat(stateManager.getDescendant("/", false, false).length, is(7));
    assertThat(stateManager.delete(stateManager.getFileInfo("/dir1"), true).length, is(6));
    assertThat(stateManager.getDescendant("/", false, false).length, is(1));

    assertThat(stateManager.mkdirs("/dir2/1/a"), is(true));
    assertThat(stateManager.mkdirs("/dir2/1/b"), is(true));
    assertThat(stateManager.mkdirs("/dir2/2/c"), is(true));
    assertThat(stateManager.getDescendant("/dir2", false, false).length, is(6));
    assertThat(stateManager.getDescendant("/", false, false).length, is(7));
    File[] filesToDelete = new File[] { stateManager.getFileInfo("/dir2/1"), stateManager.getFileInfo("/dir2/2") };
    try {
      stateManager.delete(filesToDelete, false);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
    assertThat(stateManager.getDescendant("/", false, false).length, is(7));
    assertThat(stateManager.delete(filesToDelete, true).length, is(5));
    assertThat(stateManager.getDescendant("/", false, false).length, is(2));
  }

  @Test
  public void setTimes() throws Exception {
    stateManager.mkdirs("/a");
    stateManager.setTimes("/a", 123, 456);
    assertThat(stateManager.getFileInfo("/a").mtime == 123, is(true));
    assertThat(stateManager.getFileInfo("/a").atime == 456, is(true));
  }

  @Test
  public void findBlockByFiles() throws Exception {
    stateManager.create("/file0", true, (byte) 0, 0);
    stateManager.create("/file1", true, (byte) 0, 0);

    org.apache.hadoop.hdfs.protocol.Block block = new org.apache.hadoop.hdfs.protocol.Block();
    block.set(0, 0, 0);
    stateManager.addBlockToFileBy(block, stateManager.getFileInfo("/file0").id, 0);
    stateManager.receiveBlockFromDatanodeBy(0, 0, 0, 0);
    stateManager.receiveBlockFromDatanodeBy(1, 0, 0, 0);

    block.set(1, 0, 0);
    stateManager.addBlockToFileBy(block, stateManager.getFileInfo("/file1").id, 0);
    stateManager.receiveBlockFromDatanodeBy(0, 1, 0, 0);
    File[] files = new File[] { stateManager.getFileInfo("/") };
    assertThat(stateManager.getBlocksByFiles(files).size() == 2, is(true));
  }

  @Test
  public void hearbeat() throws Exception {
    DatanodeRegistration registration = new DatanodeRegistration("0.0.0.1:54321");
    int returnCode = stateManager.registerDatanodeBy(registration, "www.tbdfs.com", System.currentTimeMillis());
    assertThat(returnCode, is(0));

    long capacity = 1024L;
    long dfsUsed = 512L;
    long remaining = capacity - dfsUsed;
    int xceiverCount = 3;
    long updateTime = System.currentTimeMillis();
    AdminStates adminState = AdminStates.NORMAL;

    stateManager.handleHeartbeat(registration, capacity, dfsUsed, remaining, xceiverCount, updateTime, adminState);

    Collection<DatanodeInfo> aliveDatanodes = stateManager.getAliveDatanodes(1000);
    assertThat(aliveDatanodes.size(), is(1));
    DatanodeInfo datanodeInfo = aliveDatanodes.iterator().next();
    assertThat(datanodeInfo.getHost(), is("0.0.0.1"));
    assertThat(datanodeInfo.getStorageID(), is(registration.getStorageID()));
    assertThat(datanodeInfo.getCapacity(), is(capacity));
    assertThat(datanodeInfo.getDfsUsed(), is(dfsUsed));
    assertThat(datanodeInfo.getRemaining(), is(remaining));
    assertThat(datanodeInfo.getXceiverCount(), is(xceiverCount));
    assertThat(datanodeInfo.getLastUpdate(), is(updateTime));
    assertThat(datanodeInfo.getHostName(), is("www.tbdfs.com"));
    assertThat(((ExDatanodeInfo) datanodeInfo).getAdminState2().equals(AdminStates.NORMAL), is(true));
  }

  @Test
  public void updateDatanode() throws Exception {
    DatanodeRegistration registration = new DatanodeRegistration("0.0.0.1:54321");

    stateManager.registerDatanodeBy(registration, "www.tbdfs.com", System.currentTimeMillis());
    Collection<DatanodeInfo> aliveDatanodes = stateManager.getAliveDatanodes(1000);
    assertThat(aliveDatanodes.size(), is(1));
    DatanodeInfo datanodeInfo = aliveDatanodes.iterator().next();
    assertThat(datanodeInfo.getHost(), is("0.0.0.1"));
    assertThat(datanodeInfo.getInfoPort(), is(registration.getInfoPort()));
    assertThat(datanodeInfo.getIpcPort(), is(registration.getIpcPort()));
    assertThat(datanodeInfo.getStorageID(), is(registration.getStorageID()));
    assertThat(datanodeInfo.getCapacity(), is(0L));
    assertThat(datanodeInfo.getDfsUsed(), is(0L));
    assertThat(datanodeInfo.getRemaining(), is(0L));
    assertThat(datanodeInfo.getXceiverCount(), is(0));
    assertThat(datanodeInfo.getHostName(), is("www.tbdfs.com"));
    assertThat(((ExDatanodeInfo) datanodeInfo).getAdminState2().equals(AdminStates.NORMAL), is(true));

    stateManager.registerDatanodeBy(registration, "www.tbdfs.com", System.currentTimeMillis());
    aliveDatanodes = stateManager.getAliveDatanodes(1000);
    assertThat(aliveDatanodes.size(), is(1));
    datanodeInfo = aliveDatanodes.iterator().next();
    assertThat(datanodeInfo.getHost(), is("0.0.0.1"));
    assertThat(datanodeInfo.getInfoPort(), is(registration.getInfoPort()));
    assertThat(datanodeInfo.getIpcPort(), is(registration.getIpcPort()));
    assertThat(datanodeInfo.getStorageID(), is(registration.getStorageID()));
    assertThat(datanodeInfo.getCapacity(), is(0L));
    assertThat(datanodeInfo.getDfsUsed(), is(0L));
    assertThat(datanodeInfo.getRemaining(), is(0L));
    assertThat(datanodeInfo.getXceiverCount(), is(0));
    assertThat(datanodeInfo.getHostName(), is("www.tbdfs.com"));
    assertThat(((ExDatanodeInfo) datanodeInfo).getAdminState2().equals(AdminStates.NORMAL), is(true));
  }

  @Test
  public void updateBlock() throws Exception {
    org.apache.hadoop.hdfs.protocol.Block block = new org.apache.hadoop.hdfs.protocol.Block(1L, 0L, 0L);
    int fileId = 1;
    int fileIndex = 0;
    stateManager.addBlockToFileBy(block, fileId, fileIndex);

    BlockEntry blockEntry;

    blockEntry = stateManager.getStoredBlockBy(block.getBlockId());

    assertThat(blockEntry.getBlockId(), is(block.getBlockId()));
    assertThat(blockEntry.getFileId(), is(fileId));
    assertThat(blockEntry.getIndex(), is(fileIndex));
    assertThat(blockEntry.getNumbytes(), is(0L));
    assertThat(blockEntry.getGenerationStamp(), is(0L));

    assertThat(blockEntry.getDatanodeIds(), hasItems(BlockInternalProtocol.NONE_DATANODE_ID));

    long numBytes = 1024L;
    long generationStamp = 1L;
    int datanodeId1 = 1;

    stateManager.receiveBlockFromDatanodeBy(datanodeId1, block.getBlockId(), numBytes, generationStamp);

    blockEntry = stateManager.getStoredBlockBy(block.getBlockId());

    assertThat(blockEntry.getBlockId(), is(block.getBlockId()));
    assertThat(blockEntry.getFileId(), is(fileId));
    assertThat(blockEntry.getIndex(), is(fileIndex));
    assertThat(blockEntry.getNumbytes(), is(numBytes));
    assertThat(blockEntry.getGenerationStamp(), is(generationStamp));
    assertThat(blockEntry.getDatanodeIds(), hasItems(datanodeId1));

    numBytes = 2048L;
    generationStamp = 2L;

    stateManager.receiveBlockFromDatanodeBy(datanodeId1, block.getBlockId(), numBytes, generationStamp);

    blockEntry = stateManager.getStoredBlockBy(block.getBlockId());

    assertThat(blockEntry.getBlockId(), is(block.getBlockId()));
    assertThat(blockEntry.getFileId(), is(fileId));
    assertThat(blockEntry.getIndex(), is(fileIndex));
    assertThat(blockEntry.getNumbytes(), is(numBytes));
    assertThat(blockEntry.getGenerationStamp(), is(generationStamp));
    assertThat(blockEntry.getDatanodeIds(), hasItems(datanodeId1));

    numBytes = 4096L;
    generationStamp = 3L;
    datanodeId1 = 2;
    stateManager.receiveBlockUpdateBytesFromDatanodeBy(datanodeId1, block.getBlockId(), numBytes, generationStamp);
    com.taobao.adfs.block.Block[] blocks =
        ((StateManager) stateManager).stateManagerInternal.findBlockById(block.getBlockId());
    assertThat(blocks.length == 2, is(true));
    assertThat(blocks[0].numbytes == 4096L, is(true));
    assertThat(blocks[1].numbytes == 4096L, is(true));
  }

  @Test
  public void uploadAndRemoveBlock() throws Exception {
    org.apache.hadoop.hdfs.protocol.Block block = new org.apache.hadoop.hdfs.protocol.Block(1L, 0L, 0L);
    int fileId = 1;
    int fileIndex = 0;
    stateManager.addBlockToFileBy(block, fileId, fileIndex);

    int datanodeId1 = 1;
    int datanodeId2 = 2;
    int datanodeId3 = 3;

    long numBytes = 1024L;
    long generationStamp = 1L;
    stateManager.receiveBlockFromDatanodeBy(datanodeId1, block.getBlockId(), numBytes, generationStamp);
    stateManager.receiveBlockFromDatanodeBy(datanodeId2, block.getBlockId(), numBytes, generationStamp);
    stateManager.receiveBlockFromDatanodeBy(datanodeId3, block.getBlockId(), numBytes, generationStamp);

    BlockEntry blockEntry = stateManager.getStoredBlockBy(block.getBlockId());

    assertThat(blockEntry.getBlockId(), is(block.getBlockId()));
    assertThat(blockEntry.getFileId(), is(fileId));
    assertThat(blockEntry.getIndex(), is(fileIndex));
    assertThat(blockEntry.getNumbytes(), is(numBytes));
    assertThat(blockEntry.getGenerationStamp(), is(generationStamp));
    assertThat(blockEntry.getDatanodeIds(), hasItems(datanodeId1, datanodeId2, datanodeId3));
    assertThat(stateManager.getBlocksByFileID(fileId), hasItem(blockEntry));

    stateManager.removeBlockReplicationOnDatanodeBy(datanodeId1, block.getBlockId());

    blockEntry = stateManager.getStoredBlockBy(block.getBlockId());
    assertThat(blockEntry.getDatanodeIds(), hasItems(datanodeId2, datanodeId3));

    stateManager.removeBlocksOfFile(fileId);

    assertThat(stateManager.getStoredBlockBy(block.getBlockId()), is(nullValue()));
  }
}
