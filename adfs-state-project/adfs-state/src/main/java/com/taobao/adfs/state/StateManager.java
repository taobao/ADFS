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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.block.BlockInternalProtocol;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.datanode.ExDatanodeInfo;
import com.taobao.adfs.distributed.DistributedClient;
import com.taobao.adfs.file.File;
import com.taobao.adfs.state.internal.StateManagerInternal;
import com.taobao.adfs.state.internal.StateManagerInternalProtocol;
import com.taobao.adfs.util.IpAddress;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManager implements StateManagerProtocol, Closeable {
  public static final Logger logger = LoggerFactory.getLogger(StateManager.class);
  Configuration conf = null;
  StateManagerInternalProtocol stateManagerInternal = null;

  public StateManager(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  void initialize() throws IOException {
    if (conf.getBoolean("distributed.client.enabled", true)) {
      conf.set("distributed.data.client.class.name", getClass().getName());
      stateManagerInternal = (StateManagerInternalProtocol) DistributedClient.getClient(conf);
    } else stateManagerInternal = new StateManagerInternal(conf, null);
  }

  long getLockerExpireTime() {
    return conf.getLong("distributed.locker.expire.time", 30000);
  }

  long getLockerTimeout() {
    return conf.getLong("distributed.locker.timeout", 60000);
  }

  // for Closeable

  @Override
  public void close() throws IOException {
    if (stateManagerInternal != null && conf.getBoolean("distributed.client.enabled", true))
      ((Closeable) stateManagerInternal).close();
  }

  // for StateManagerProtocol

  @Override
  public void format() throws IOException {
    stateManagerInternal.format();
  }

  @Override
  public File[] deleteFileAndBlockByPath(String path, boolean recursive) throws IOException {
    List<File> files = new ArrayList<File>();
    for (Object object : stateManagerInternal.deleteFileAndBlockByPath(path, recursive)) {
      if (object != null && object instanceof File && !((File) object).isDir()) files.add((File) object);
    }
    return files.toArray(new File[files.size()]);
  }

  // for DistributedLockerProtocol

  @Override
  public boolean lock(long expireTime, long timeout, Object... objects) throws IOException {
    return stateManagerInternal.lock(null, expireTime, timeout, objects) != null;
  }

  @Override
  public boolean tryLock(long expireTime, Object... objects) throws IOException {
    return stateManagerInternal.tryLock(null, expireTime, objects) != null;
  }

  @Override
  public boolean unlock(Object... objects) throws IOException {
    return stateManagerInternal.unlock(null, objects) != null;
  }

  // for FileProtocol

  @Override
  public boolean mkdirs(String path) throws IOException {
    // follow HADOOP style
    stateManagerInternal.insertFileByPath(path, 0, -1, (byte) 0, false);
    return true;
  }

  @Override
  public File create(String path, boolean overwrite, byte replication, int blockSize) throws IOException {
    File[] files = stateManagerInternal.insertFileByPath(path, blockSize, 0, replication, overwrite);
    return files[files.length - 1];
  }

  @Override
  public boolean complete(int id, long length) throws IOException {
    stateManagerInternal.updateFileByFile(new File(id, 0, null, length, 0, (byte) 0, 0, 0, 0), 0x04);
    return true;
  }

  @Override
  public File[] rename(String path, String targetPath) throws IOException {
    return stateManagerInternal.updateFileByPathAndPath(path, targetPath);
  }

  @Override
  public void setTimes(String path, long mtime, long atime) throws IOException {
    stateManagerInternal.updateFileByPathAndFile(path, new File(0, null, 0, 0, (byte) 0, atime, mtime, 0), 0x60);
  }

  @Override
  public short setReplication(String path, byte replication) throws IOException {
    File oldFile = stateManagerInternal.findFileByPath(path);
    if (oldFile == null) throw new IOException("fail to get file " + oldFile);
    if (oldFile.replication == replication) return replication;
    stateManagerInternal.updateFileByPathAndFile(path, new File(0, null, 0, 0, replication, 0, 0, 0), 0x10);
    return oldFile.replication;
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException {
  }

  @Override
  public File[] delete(String path) throws IOException {
    return delete(path, true);
  }

  @Override
  public File[] delete(String path, boolean recursive) throws IOException {
    List<File> deletedFiles = new ArrayList<File>();
    for (File file : stateManagerInternal.deleteFileByPath(path, recursive)) {
      if (file != null && !file.isDir()) deletedFiles.add(file);
    }
    return deletedFiles.toArray(new File[deletedFiles.size()]);
  }

  @Override
  public File[] delete(File file, boolean recursive) throws IOException {
    return stateManagerInternal.deleteFileByFile(file, recursive);
  }

  @Override
  public File[] delete(File[] files, boolean recursive) throws IOException {
    return stateManagerInternal.deleteFileByFiles(files, recursive);
  }

  @Override
  public File getFileInfo(String path) throws IOException {
    return stateManagerInternal.findFileByPath(path);
  }

  @Override
  public File getFileInfo(int id) throws IOException {
    return stateManagerInternal.findFileById(id);
  }

  @Override
  public File[] getListing(String path) throws IOException {
    return stateManagerInternal.findFileChildrenByPath(path);
  }

  @Override
  public File[] getDescendant(String path, boolean exculudeDir, boolean includeSelfAnyway) throws IOException {
    return stateManagerInternal.findFileDescendantByPath(path, exculudeDir, includeSelfAnyway);
  }

  @Override
  public File[] getDescendant(File file, boolean exculudeDir, boolean includeSelfAnyway) throws IOException {
    return stateManagerInternal.findFileDescendantByFile(file, exculudeDir, includeSelfAnyway);
  }

  // for BlockProtocol

  @Override
  public BlockEntry getStoredBlockBy(long id) throws IOException {
    Block[] blocks = stateManagerInternal.findBlockById(id);
    if (blocks.length == 0) return null;
    return BlockEntry.getBlockEntries(blocks).get(0);
  }

  @Override
  public BlockEntry getStoredBlockBy(long id, long generationStamp) throws IOException {
    BlockEntry blockEntry = getStoredBlockBy(id);
    if (blockEntry == null || blockEntry.getGenerationStamp() != generationStamp) return null;
    return blockEntry;
  }

  @Override
  public List<BlockEntry> getBlocksByFileID(int fileId) throws IOException {
    List<BlockEntry> blockEntries = BlockEntry.getBlockEntries(stateManagerInternal.findBlockByFileId(fileId));
    Collections.sort(blockEntries);
    return blockEntries;
  }

  @Override
  public List<BlockEntry> getBlocksByFiles(File[] files) throws IOException {
    return BlockEntry.getBlockEntries(stateManagerInternal.findBlockByFiles(files));
  }

  @Override
  public void addBlockToFileBy(org.apache.hadoop.hdfs.protocol.Block block, int fileId, int fileIndex)
      throws IOException {
    addBlockToFile(block, fileId, fileIndex, false);
  }

  public void addBlockToFile(org.apache.hadoop.hdfs.protocol.Block block, int fileId, int fileIndex,
      boolean removeBeforeAdding) throws IOException {
    if (removeBeforeAdding) stateManagerInternal.deleteBlockById(block.getBlockId());
    Block b =
        new Block(block.getBlockId(), block.getNumBytes(), block.getGenerationStamp(), fileId,
            BlockInternalProtocol.NONE_DATANODE_ID, fileIndex);
    stateManagerInternal.insertBlockByBlock(b);
  }

  @Override
  public Collection<BlockEntry> getAllBlocksOnDatanode(int datanodeId) throws IOException {
    return BlockEntry.getBlockEntries(stateManagerInternal.findBlockByDatanodeId(datanodeId));
  }

  @Override
  public boolean isBlockIdExists(long blockId) throws IOException {
    return stateManagerInternal.findBlockById(blockId).length > 0;
  }

  @Override
  public void receiveBlockFromDatanodeBy(int datanodeId, long blockId, long numbytes, long generationStamp)
      throws IOException {
    receiveBlockFromDatanodeByInternal(datanodeId, blockId, numbytes, generationStamp, false);
  }

  @Override
  public void receiveBlockUpdateBytesFromDatanodeBy(int datanodeId, long blockId, long numbytes, long generationStamp)
      throws IOException {
    receiveBlockFromDatanodeByInternal(datanodeId, blockId, numbytes, generationStamp, true);
  }

  public void receiveBlockFromDatanodeByInternal(int datanodeId, long blockId, long numbytes, long generationStamp,
      boolean updateNumbytesForAllDatanodes) throws IOException {
    Block[] blocks = stateManagerInternal.findBlockById(blockId);
    if (blocks.length == 0) throw new IllegalStateException("not existed block id=" + blockId);

    Block newBlock = new Block(blockId, numbytes, generationStamp, blocks[0].fileId, datanodeId, blocks[0].fileIndex);
    boolean isBlockOnNoneDatanode = false;
    boolean isBlockOnRealDatanode = false;
    for (Block block : blocks) {
      if (block.datanodeId == BlockInternalProtocol.NONE_DATANODE_ID) {
        isBlockOnNoneDatanode = true;
      } else if (block.datanodeId == datanodeId) {
        isBlockOnRealDatanode = true;
        stateManagerInternal.updateBlockByBlock(newBlock);
      } else if (updateNumbytesForAllDatanodes) {
        block.numbytes = numbytes;
        stateManagerInternal.updateBlockByBlock(block);
      }
    }

    if (!isBlockOnRealDatanode) stateManagerInternal.insertBlockByBlock(newBlock);
    if (isBlockOnNoneDatanode) {
      try {
        stateManagerInternal.deleteBlockByIdAndDatanodeId(blockId, BlockInternalProtocol.NONE_DATANODE_ID);
      } catch (IOException e) {
        // when two namenodes call this function at the same time, the second namenode will fail to delete it
      }
    }
  }

  @Override
  public void removeBlock(long blockId) throws IOException {
    stateManagerInternal.deleteBlockById(blockId);
  }

  @Override
  public void removeBlockReplicationOnDatanodeBy(int datanodeId, long blockId) throws IOException {
    Block[] blocks = stateManagerInternal.findBlockById(blockId);
    for (Block block : blocks) {
      if (block.id == blockId && block.datanodeId == datanodeId) {
        if (blocks.length == 1) {
          block.datanodeId = -1;
          stateManagerInternal.insertBlockByBlock(block);
        }
        stateManagerInternal.deleteBlockByIdAndDatanodeId(blockId, datanodeId);
      }
    }
  }

  @Override
  public void removeBlocksOfFile(int fileId) throws IOException {
    stateManagerInternal.deleteBlockByFileId(fileId);
  }

  @Override
  public Collection<Integer> getDatanodeList(long id) throws IOException {
    try {
      BlockEntry blockEntry = getStoredBlockBy(id);
      if (blockEntry == null) return null;
      return blockEntry.getDatanodeIds();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  // for DatanodeProtocol

  @Override
  public int registerDatanodeBy(DatanodeRegistration registration, String hostName, long updateTime) throws IOException {
    int datanodId = IpAddress.getAddress(registration.getHost());
    Datanode[] datanodes = stateManagerInternal.findDatanodeById(datanodId);
    if (datanodes.length == 0) {
      Datanode datanode =
          new Datanode(datanodId, registration.getName(), registration.getStorageID(), registration.getIpcPort(),
              registration.getInfoPort(), registration.getVersion(), registration.storageInfo.namespaceID,
              registration.storageInfo.cTime, 0L, 0L, 0L, updateTime, 0, NetworkTopology.DEFAULT_RACK, hostName,
              ExDatanodeInfo.getAdminState(AdminStates.NORMAL));
      stateManagerInternal.insertDatanodeByDatanode(datanode);
      return 0;
    } else {
      datanodes[0].name = registration.getName();
      datanodes[0].storageId = registration.getStorageID();
      datanodes[0].ipcPort = registration.getIpcPort();
      datanodes[0].infoPort = registration.getIpcPort();
      datanodes[0].layoutVersion = registration.getVersion();
      datanodes[0].lastUpdated = updateTime;
      datanodes[0].hostName = hostName;
      datanodes[0].adminState = ExDatanodeInfo.getAdminState(AdminStates.NORMAL);
      stateManagerInternal.updateDatanodeByDatanode(datanodes[0]);
      return 1;
    }
  }

  @Override
  public boolean handleHeartbeat(DatanodeRegistration registration, long capacity, long dfsUsed, long remaining,
      int xceiverCount, long updateTime, AdminStates adminState) throws IOException {
    int datanodId = IpAddress.getAddress(registration.getHost());
    Datanode[] datanodes = stateManagerInternal.findDatanodeById(datanodId);
    if (datanodes.length == 0) throw new IOException("not registered datanode id=" + datanodId);
    datanodes[0].capacity = capacity;
    datanodes[0].dfsUsed = dfsUsed;
    datanodes[0].remaining = remaining;
    datanodes[0].xceiverCount = xceiverCount;
    datanodes[0].lastUpdated = updateTime;
    datanodes[0].adminState = ExDatanodeInfo.getAdminState(adminState);
    stateManagerInternal.updateDatanodeByDatanode(datanodes[0]);
    return true;
  }

  @Override
  public Collection<DatanodeInfo> getAliveDatanodes(long datanodeExpireInterval) throws IOException {
    long aliveTimeFrom = System.currentTimeMillis() - datanodeExpireInterval;
    Datanode[] datanodes = stateManagerInternal.findDatanodeByUpdateTime(aliveTimeFrom);
    Collection<DatanodeInfo> datanodeInfos = new HashSet<DatanodeInfo>();
    for (Datanode datanode : datanodes) {
      datanodeInfos.add(new ExDatanodeInfo(datanode));
    }
    return datanodeInfos;
  }

  @Override
  public boolean isDatanodeStorageIDExists(String storageId) throws IOException {
    return stateManagerInternal.findDatanodeByStorageId(storageId).length > 0;
  }

  // for utilities

  static public FileStatus fileToFileStatus(File file) {
    if (file == null) return null;
    return new FileStatus(file.length, file.isDir(), file.replication, file.blockSize, file.mtime, file.atime, null,
        String.valueOf(file.owner), "", new Path(file.path));
  }

  static public FileStatus[] fileArrayToFileStatusArray(File[] files) {
    if (files == null) return null;
    FileStatus[] fileStatuses = new FileStatus[files.length];
    for (int i = 0; i < files.length; ++i) {
      fileStatuses[i] = fileToFileStatus(files[i]);
    }
    return fileStatuses;
  }

  static public String getTrashPath() {
    return "/.trash";
  }

  static public String getTrashPath(File file) {
    if (file == null || file.name == null || file.isRootById() || file.isRootByParentIdAndName()) return null;
    return String.format("%s/%+011d-%s", getTrashPath(), file.id, file.name);
  }
}
