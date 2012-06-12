/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.block.BlockRepository;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.datanode.DatanodeRepository;
import com.taobao.adfs.distributed.DistributedOperationQueue;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepository;
import com.taobao.adfs.lease.Lease;
import com.taobao.adfs.lease.LeaseRepository;
import com.taobao.adfs.util.IpAddress;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManager {
  public static final Logger logger = LoggerFactory.getLogger(DistributedOperationQueue.class);

  FileRepository fileRepository = null;
  BlockRepository blockRepository = null;
  DatanodeRepository datanodeRepository = null;
  LeaseRepository leaseRepository = null;
  private static long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private static long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;

  public void setLeasePeriod(long softLimit, long hardLimit) {
    StateManager.softLimit = softLimit;
    StateManager.hardLimit = hardLimit;
  }

  /**
   * StateManager constructor.
   */
  public StateManager(FileRepository fileRepository, BlockRepository blockRepository,
      DatanodeRepository datanodeRepository, LeaseRepository leaseRepository) {
    this.fileRepository = fileRepository;
    this.blockRepository = blockRepository;
    this.datanodeRepository = datanodeRepository;
    this.leaseRepository = leaseRepository;
  }

  // ///////////////////////////////////////////////////
  // FileProtocol
  // ///////////////////////////////////////////////////
  /**
   * Directory or File
   */
  public boolean isDir(String path) throws IOException {
    File file = findFileByPath(path);
    return file != null && file.isDir();
  }

  /**
   * get file status
   * 
   * @param path
   *          file path
   * @return
   * @throws IOException
   */
  public HdfsFileStatus getFileInfo(String path) throws IOException {
    return adfsFileToHdfsFileStatus(findFileByPath(path));
  }

  /**
   * use blockId map file 
   * 
   * @param blockId
   * @return
   * @throws IOException
   */
  public HdfsFileStatus getFileInfoByBlockId(long blockId) throws IOException {
    BlockEntry blockEntry = getBlockEntryByBlockId(blockId);
    if (blockEntry == null) return null;
    else return getFileInfo(blockEntry.getFileId());
  }

  /**
   * use fileId to find file
   * 
   * @param id fileId 
   * @return
   * @throws IOException
   */
  public HdfsFileStatus getFileInfo(long id) throws IOException {
    return adfsFileToHdfsFileStatus(findFileById(id));
  }

  public HdfsFileStatus[] getListing(String path) throws IOException {
    return adfsFileArrayToHdfsFileStatusArray(findFileChildrenByPath(path));
  }

  // ///////////////////////////////////////////////////
  // BlockProtocol
  // ///////////////////////////////////////////////////

  public BlockEntry getBlockEntryByBlockId(long blockId) throws IOException {
    List<BlockEntry> blockEntryList = BlockEntry.getBlockEntryList(findBlockById(blockId));
    return (blockEntryList == null || blockEntryList.isEmpty()) ? null : blockEntryList.get(0);
  }

  public org.apache.hadoop.hdfs.protocol.Block[] getHadoopBlockArray(String path) throws IOException {
    File file = findFileByPath(path);
    if (file == null) return null;
    List<BlockEntry> entryList = getBlockEntryListByFileId(file.id);
    return BlockEntry.getHadoopBlockArray(entryList);
  }

  public List<org.apache.hadoop.hdfs.protocol.Block> getHadoopBlocks(String path) throws IOException {
    File file = findFileByPath(path);
    if (file == null) return null;
    List<BlockEntry> entryList = getBlockEntryListByFileId(file.id);
    return BlockEntry.getHadoopBlock(entryList);
  }

  public List<org.apache.hadoop.hdfs.protocol.Block> getHadoopBlocks(long fileId) throws IOException {
    List<BlockEntry> entryList = getBlockEntryListByFileId(fileId);
    return BlockEntry.getHadoopBlock(entryList);
  }

  public org.apache.hadoop.hdfs.protocol.Block[] getHadoopBlockArray(long fileId) throws IOException {
    List<BlockEntry> entryList = getBlockEntryListByFileId(fileId);
    return BlockEntry.getHadoopBlockArray(entryList);
  }

  static List<org.apache.hadoop.hdfs.protocol.Block> blocksToHadoopBlocks(Block[] blocks) {
    List<org.apache.hadoop.hdfs.protocol.Block> hadoopBlocks = new ArrayList<org.apache.hadoop.hdfs.protocol.Block>();
    if (blocks == null) return hadoopBlocks;
    for (Block block : blocks) {
      if (block == null) continue;
      org.apache.hadoop.hdfs.protocol.Block hadoopBlock = new org.apache.hadoop.hdfs.protocol.Block();
      hadoopBlock.set(block.id, block.length, block.generationStamp);

      hadoopBlocks.add(hadoopBlock);
    }
    return hadoopBlocks;
  }

  public List<BlockEntry> getBlockEntryListOfFile(String path) throws IOException {
    if (path == null) return null;
    File file = findFileByPath(path);
    if (file == null) return null;
    return getBlockEntryListByFileId(file.id);
  }

  public List<BlockEntry> getBlockEntryListByFileId(long fileId) throws IOException {
    List<BlockEntry> blockEntryList = BlockEntry.getBlockEntryList(findBlockByFileId(fileId));
    Collections.sort(blockEntryList);
    return blockEntryList;
  }

  public static BlockInfo adfsBlockEntryToBlockInfo(BlockEntry blockEntry) throws IOException {
    if (blockEntry == null) return null;
    return new BlockInfo(blockEntry.getHdfsBlock(), 0);
  }

  static public BlockInfo[] adfsBlockEntryListToHadoopBlockInfoArray(List<BlockEntry> blockEntryList)
      throws IOException {
    if (blockEntryList == null) return null;
    BlockInfo[] blockInfoArray = new BlockInfo[blockEntryList.size()];
    for (int i = 0; i < blockInfoArray.length; ++i) {
      blockInfoArray[i] = adfsBlockEntryToBlockInfo(blockEntryList.get(i));
    }
    return blockInfoArray;
  }

  static public Map<org.apache.hadoop.hdfs.protocol.Block, BlockInfo> adfsBlockEntryListToHadoopBlockInfoMap(
      List<BlockEntry> blockEntryList) throws IOException {
    if (blockEntryList == null) return null;
    Map<org.apache.hadoop.hdfs.protocol.Block, BlockInfo> blockInfoMap =
        new HashMap<org.apache.hadoop.hdfs.protocol.Block, BlockInfo>(blockEntryList.size());
    for (BlockEntry blockEntry : blockEntryList) {
      if (blockEntry == null) continue;
      blockInfoMap.put(blockEntry.getHdfsBlock(), adfsBlockEntryToBlockInfo(blockEntry));
    }
    return blockInfoMap;
  }

  public List<BlockEntry> getBlocksByFiles(List<File> fileList) throws IOException {
    return BlockEntry.getBlockEntryList(findBlockByFiles(fileList));
  }

  public BlockEntry getLastBlockEntryByFileId(long fileId) throws IOException {
    List<BlockEntry> blockEntryList = getBlockEntryListByFileId(fileId);
    return BlockEntry.getLastBlockEntry(blockEntryList);
  }

  // for DatanodeProtocol
  public NetworkTopology clusterMap = null;

  Map<Integer, Map<Integer, DatanodeDescriptor>> addressToDatanodeDescriptor =
      new ConcurrentHashMap<Integer, Map<Integer, DatanodeDescriptor>>();
  Map<String, DatanodeDescriptor> storageIdToDatanodeDescriptor = new ConcurrentHashMap<String, DatanodeDescriptor>();

  public List<DatanodeDescriptor> getDatanodeDescriptorList(boolean onlyAlive) throws IOException {
    reloadDatanodeDescriptorMaps(false);
    List<DatanodeDescriptor> datanodeDescriptorList =
        new ArrayList<DatanodeDescriptor>(addressToDatanodeDescriptor.size() * 2);
    for (Map<Integer, DatanodeDescriptor> datanodeDescriptorMap : addressToDatanodeDescriptor.values()) {
      for (DatanodeDescriptor datanodeDescriptor : datanodeDescriptorMap.values()) {
        if (!onlyAlive || datanodeDescriptor.isAlive) datanodeDescriptorList.add(datanodeDescriptor);
      }
    }
    return datanodeDescriptorList;
  }

  public DatanodeDescriptor getDatanodeDescriptorByStorageId(String storageId) throws IOException {
    if (storageId == null) return null;
    reloadDatanodeDescriptorMaps(false);
    return storageIdToDatanodeDescriptor.get(storageId);
  }

  public DatanodeDescriptor getDatanodeDescriptorByDatanodeId(long datanodeId) throws IOException {
    reloadDatanodeDescriptorMaps(false);
    Map<Integer, DatanodeDescriptor> portToDatanodeDescriptorMap =
        addressToDatanodeDescriptor.get(IpAddress.getIp(datanodeId));
    if (portToDatanodeDescriptorMap == null || portToDatanodeDescriptorMap.isEmpty()) return null;
    return portToDatanodeDescriptorMap.get(IpAddress.getPort(datanodeId));
  }

  public DatanodeDescriptor[] getDatanodeDescriptorArrayByDatanodeIdList(List<Long> datanodeIdList) throws IOException {
    List<DatanodeDescriptor> datanodeDescriptorList = getDatanodeDescriptorListByDatanodeIdList(datanodeIdList);
    if (datanodeDescriptorList == null) return null;
    else return datanodeDescriptorList.toArray(new DatanodeDescriptor[datanodeDescriptorList.size()]);
  }

  public List<DatanodeDescriptor> getDatanodeDescriptorListByDatanodeIdList(List<Long> datanodeIdList)
      throws IOException {
    if (datanodeIdList == null) return null;
    List<DatanodeDescriptor> datanodeDescriptorList = new ArrayList<DatanodeDescriptor>(datanodeIdList.size());
    for (Long datanodeId : datanodeIdList) {
      if (datanodeId == null) continue;
      DatanodeDescriptor datanodeDescriptor = getDatanodeDescriptorByDatanodeId(datanodeId);
      if (datanodeDescriptor == null) continue;
      datanodeDescriptorList.add(datanodeDescriptor);
    }
    return datanodeDescriptorList;
  }

  public DatanodeDescriptor[] getDatanodeDescriptorArrayByBlockList(List<Block> blockList) throws IOException {
    List<DatanodeDescriptor> datanodeDescriptorList = getDatanodeDescriptorListByBlockList(blockList);
    if (datanodeDescriptorList == null) return null;
    else return datanodeDescriptorList.toArray(new DatanodeDescriptor[datanodeDescriptorList.size()]);
  }

  public List<DatanodeDescriptor> getDatanodeDescriptorListByBlockList(List<Block> blockList) throws IOException {
    if (blockList == null) return null;
    List<DatanodeDescriptor> datanodeDescriptorList = new ArrayList<DatanodeDescriptor>(blockList.size());
    for (Block block : blockList) {
      if (block == null) continue;
      DatanodeDescriptor datanodeDescriptor = getDatanodeDescriptorByDatanodeId(block.datanodeId);
      if (datanodeDescriptor == null) continue;
      datanodeDescriptorList.add(datanodeDescriptor);
    }
    return datanodeDescriptorList;
  }

  public DatanodeDescriptor getDatanodeDescriptorByDatanodeIp(int datanodeIp) throws IOException {
    reloadDatanodeDescriptorMaps(false);
    Map<Integer, DatanodeDescriptor> portToDatanodeDescriptorMap = addressToDatanodeDescriptor.get(datanodeIp);
    if (portToDatanodeDescriptorMap == null || portToDatanodeDescriptorMap.isEmpty()) return null;
    int randomIndex = (int) System.currentTimeMillis() % portToDatanodeDescriptorMap.size();
    if (randomIndex < 0) randomIndex = -randomIndex;
    java.util.Iterator<DatanodeDescriptor> it = portToDatanodeDescriptorMap.values().iterator();
    for (; randomIndex-- > 0;) {
      it.next();
    }
    return it.next();
  }

  public DatanodeDescriptor getDatanodeDescriptorByName(String name) throws IOException {
    if (name == null) return null;
    String[] hostAndPort = name.split(":", 2);
    if (hostAndPort.length == 0 || hostAndPort[0] == null) return null;
    if (hostAndPort.length == 2 && hostAndPort[1] == null) return null;
    int ipValue = IpAddress.getAddress(hostAndPort[0]);
    if (hostAndPort.length == 1) {
      return getDatanodeDescriptorByDatanodeIp(ipValue);
    } else {
      reloadDatanodeDescriptorMaps(false);
      Map<Integer, DatanodeDescriptor> portToDatanodeDescriptorMap = addressToDatanodeDescriptor.get(ipValue);
      if (portToDatanodeDescriptorMap == null || portToDatanodeDescriptorMap.isEmpty()) return null;
      else return portToDatanodeDescriptorMap.get(Integer.valueOf(hostAndPort[1]));
    }
  }

  public void reloadDatanodeDescriptorMaps(boolean force) throws IOException {
    if (force || addressToDatanodeDescriptor.isEmpty()) {
      synchronized (addressToDatanodeDescriptor) {
        if (addressToDatanodeDescriptor.isEmpty()) {
          long clusterLoad = 0;
          long clusterCapacity = 0L;
          long clusterRemaining = 0L;
          long clusterDfsUsed = 0L;
          long clusterLiveDatanode = 0L;
          addressToDatanodeDescriptor.clear();
          storageIdToDatanodeDescriptor.clear();
          clusterMap = new NetworkTopology();
          for (Datanode datanode : datanodeRepository.findByIdGreateOrEqual(Long.MIN_VALUE)) {
            Map<Integer, DatanodeDescriptor> portToDatanodeDescriptorMap =
                addressToDatanodeDescriptor.get(datanode.getIp());
            if (portToDatanodeDescriptorMap == null) {
              portToDatanodeDescriptorMap = new ConcurrentHashMap<Integer, DatanodeDescriptor>();
              addressToDatanodeDescriptor.put(datanode.getIp(), portToDatanodeDescriptorMap);
            }
            DatanodeDescriptor datanodeDescriptor = adfsDatanodeToDatanodeDescriptor(datanode);
            portToDatanodeDescriptorMap.put(datanode.getPort(), datanodeDescriptor);
            storageIdToDatanodeDescriptor.put(datanodeDescriptor.getStorageID(), datanodeDescriptor);
            if (AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState())) clusterMap.add(datanodeDescriptor);
            clusterLoad += datanodeDescriptor.getXceiverCount();
            clusterCapacity += datanodeDescriptor.getCapacity();
            clusterRemaining += datanodeDescriptor.getRemaining();
            clusterDfsUsed += datanodeDescriptor.getDfsUsed();
            clusterLiveDatanode += datanodeDescriptor.isAlive ? 1 : 0;
          }
          this.clusterLoad = clusterLoad;
          this.clusterCapacity = clusterCapacity;
          this.clusterRemaining = clusterRemaining;
          this.clusterDfsUsed = clusterDfsUsed;
          this.clusterLiveDatanode = clusterLiveDatanode;
        }
      }
    }
  }

  public static DatanodeDescriptor adfsDatanodeToDatanodeDescriptor(Datanode datanode) {
    if (datanode == null) return null;
    DatanodeDescriptor datanodeDescriptor = new DatanodeDescriptor();
    datanodeDescriptor.id = datanode.id;
    datanodeDescriptor.name = datanode.name;
    datanodeDescriptor.storageID = datanode.storageId;
    datanodeDescriptor.infoPort = datanode.infoPort;
    datanodeDescriptor.ipcPort = datanode.ipcPort;
    datanodeDescriptor.capacity = datanode.capacity;
    datanodeDescriptor.dfsUsed = datanode.dfsUsed;
    datanodeDescriptor.remaining = datanode.remaining;
    datanodeDescriptor.lastUpdate = datanode.lastUpdated;
    datanodeDescriptor.xceiverCount = datanode.xceiverCount;
    datanodeDescriptor.location = datanode.location;
    datanodeDescriptor.setAdminState(AdminStates.valueOf(datanode.adminState));
    datanodeDescriptor.isAlive =
        datanodeDescriptor.getAdminState() == null || !AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState());
    return datanodeDescriptor;
  }

  public static Datanode datanodeDescriptorToAdfsDatanode(DatanodeDescriptor datanodeDescriptor) {
    if (datanodeDescriptor == null) return null;
    Datanode datanode = new Datanode();
    datanode.id = datanodeDescriptor.getId();
    datanode.name = datanodeDescriptor.getName();
    datanode.storageId = datanodeDescriptor.getStorageID();
    datanode.infoPort = datanodeDescriptor.getInfoPort();
    datanode.ipcPort = datanodeDescriptor.getIpcPort();
    datanode.capacity = datanodeDescriptor.getCapacity();
    datanode.dfsUsed = datanodeDescriptor.getDfsUsed();
    datanode.remaining = datanodeDescriptor.getRemaining();
    datanode.lastUpdated = datanodeDescriptor.getLastUpdate();
    datanode.xceiverCount = datanodeDescriptor.getXceiverCount();
    datanode.location = datanodeDescriptor.getNetworkLocation();
    datanode.adminState = datanodeDescriptor.getAdminState().toString();
    return datanode;
  }

  public void updateDatanodeByDatanodeDescriptor(DatanodeDescriptor datanodeDescriptor) throws IOException {
    if (datanodeDescriptor == null) return;
    // try to insert
    if (getDatanodeDescriptorByDatanodeId(datanodeDescriptor.getId()) == null) {
      synchronized (addressToDatanodeDescriptor) {
        int ip = IpAddress.getIp(datanodeDescriptor.getId());
        int port = IpAddress.getPort(datanodeDescriptor.getId());
        if (getDatanodeDescriptorByDatanodeId(datanodeDescriptor.getId()) == null) {
          Map<Integer, DatanodeDescriptor> portToDatanodeDescriptorMap = addressToDatanodeDescriptor.get(ip);
          if (portToDatanodeDescriptorMap == null) {
            portToDatanodeDescriptorMap = new ConcurrentHashMap<Integer, DatanodeDescriptor>();
            addressToDatanodeDescriptor.put(ip, portToDatanodeDescriptorMap);
          }
          portToDatanodeDescriptorMap.put(port, datanodeDescriptor);
          storageIdToDatanodeDescriptor.put(datanodeDescriptor.getStorageID(), datanodeDescriptor);
          clusterMap.remove(datanodeDescriptor);
          if (AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState())) clusterMap.add(datanodeDescriptor);
        }
        datanodeRepository.insert(datanodeDescriptorToAdfsDatanode(datanodeDescriptor), false);
        return;
      }
    }

    // update data node infoF
    synchronized (addressToDatanodeDescriptor) {
      if (AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState()) && !clusterMap.contains(datanodeDescriptor)) {
        clusterMap.add(datanodeDescriptor);
      }
      if (!AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState()) && clusterMap.contains(datanodeDescriptor)) {
        clusterMap.remove(datanodeDescriptor);
      }
      DatanodeDescriptor datanodeDescriptorByStorageId =
          getDatanodeDescriptorByStorageId(datanodeDescriptor.getStorageID());
      if (datanodeDescriptorByStorageId != getDatanodeDescriptorByStorageId(datanodeDescriptor.getStorageID())) {
        storageIdToDatanodeDescriptor.put(datanodeDescriptor.getStorageID(), datanodeDescriptor);
      }
    }
    datanodeRepository.update(datanodeDescriptorToAdfsDatanode(datanodeDescriptor), Datanode.ALL);
  }

  long clusterLoad = 0;
  long clusterCapacity = 0L;
  long clusterRemaining = 0L;
  long clusterDfsUsed = 0L;
  long clusterLiveDatanode = 0L;
  volatile long clusterStatisticsLastUpdateTime = 0;
  AtomicBoolean clusterStatisticsIsUpdating = new AtomicBoolean(false);

  private void updateClusterStatistics() {
    if (System.currentTimeMillis() - clusterStatisticsLastUpdateTime > 1000) {
      if (!clusterStatisticsIsUpdating.compareAndSet(false, true)) return;
      try {
        long clusterLoad = 0;
        long clusterCapacity = 0L;
        long clusterRemaining = 0L;
        long clusterDfsUsed = 0L;
        long clusterLiveDatanode = 0L;
        for (DatanodeDescriptor datanodeDescriptor : getDatanodeDescriptorList(true)) {
          if (datanodeDescriptor == null) continue;
          clusterLoad += datanodeDescriptor.getXceiverCount();
          clusterCapacity += datanodeDescriptor.getCapacity();
          clusterRemaining += datanodeDescriptor.getRemaining();
          clusterDfsUsed += datanodeDescriptor.getDfsUsed();
          clusterLiveDatanode += datanodeDescriptor.isAlive ? 1 : 0;
        }
        this.clusterLoad += clusterLoad;
        this.clusterCapacity += clusterCapacity;
        this.clusterRemaining += clusterRemaining;
        this.clusterDfsUsed += clusterDfsUsed;
        this.clusterLiveDatanode += clusterLiveDatanode;
        this.clusterStatisticsLastUpdateTime = System.currentTimeMillis();
      } catch (Throwable t) {
        Utilities.logWarn(logger, t);
      } finally {
        clusterStatisticsIsUpdating.set(false);
      }
    }
  }

  public long getClusterLoad() {
    updateClusterStatistics();
    return clusterLoad;
  }

  public long getClusterCapacity() {
    updateClusterStatistics();
    return clusterCapacity;
  }

  public long getClusterRemaining() {
    updateClusterStatistics();
    return clusterRemaining;
  }

  public long getClusterDfsUsed() {
    updateClusterStatistics();
    return clusterDfsUsed;
  }

  public long getClusterLiveDatanode() {
    updateClusterStatistics();
    return clusterLiveDatanode;
  }

  public long getClusterDeadDatanode() {
    return addressToDatanodeDescriptor.size() - getClusterLiveDatanode();
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

  // for StateManagerInternal

  // for FileProtocol

  public long countFile() throws IOException {
    return fileRepository.count();
  }

  public File insertFileByPath(String path, int blockSize, long length, byte replication, boolean overwrite,
      String leaseHolder) throws IOException {
    String[] namesInPath = Utilities.getNamesInPath(path);
    namesInPath[0] = File.ROOT.name;
    File parentFileInPath = File.ROOT;
    for (int i = 0; i < namesInPath.length; ++i) {
      boolean isLastFile = i == namesInPath.length - 1;
      String nameInPath = namesInPath[i];
      File fileInPath = new File();
      fileInPath.parentId = parentFileInPath.id;
      fileInPath.name = nameInPath;
      fileInPath.atime = System.currentTimeMillis();
      fileInPath.mtime = fileInPath.atime;
      fileInPath.length = isLastFile ? length : -1;
      fileInPath.blockSize = isLastFile ? blockSize : 0;
      fileInPath.replication = isLastFile ? replication : 0;
      fileInPath.leaseHolder = isLastFile ? leaseHolder : null;
      fileInPath = fileRepository.insert(fileInPath, isLastFile ? overwrite : false);
      parentFileInPath = fileInPath;
    }
    parentFileInPath.path = Utilities.getPathInName(namesInPath, namesInPath.length - 1);
    return parentFileInPath;
  }

  public File updateFileByFile(File file, int fieldsIndication) throws IOException {
    if (file == null) throw new IOException("file is null");
    File newFile = fileRepository.update(file, fieldsIndication);
    if (file.path != null && (fieldsIndication & (File.PARENTID | File.NAME)) == 0) newFile.path = file.path;
    else newFile.path = findFileById(newFile.id).path;
    return newFile;
  }

  public List<File> deleteFileByPath(String path, boolean recursive) throws IOException {
    return deleteFileByFile(findFileByPath(path), recursive);
  }

  public List<File> deleteFileByFile(File file, boolean recursive) throws IOException {
    if (file == null) return new ArrayList<File>(0);
    if (file.isDir() && !recursive && !fileRepository.findByParentId(file.id).isEmpty())
      throw new IOException("fail to delete " + file.path + ": find children");
    List<File> deletedFiles = new ArrayList<File>();
    if (file.isDir()) {
      for (File fileToDelete : findFileChildrenById(file.id)) {
        if (fileToDelete == null) continue;
        else if (!fileToDelete.isDir()) {
          fileToDelete = fileRepository.delete(fileToDelete);
          if (fileToDelete != null) {
            fileToDelete.path = file.path + "/" + fileToDelete.name;
            deletedFiles.add(fileToDelete);
          }
        } else {
          for (File deletedFile : deleteFileByFile(fileToDelete, true)) {
            deletedFiles.add(deletedFile);
          }
        }
      }
    }

    File deleteFile = fileRepository.delete(file);
    if (deleteFile != null) {
      deleteFile.path = file.path;
      deletedFiles.add(deleteFile);
    }
    return deletedFiles;
  }

  public List<File> deleteFileByFiles(File[] files, boolean recursive) throws IOException {
    if (files == null) return new ArrayList<File>(0);
    List<File> deleteFiles = new ArrayList<File>();
    for (File file : files) {
      deleteFiles.addAll(deleteFileByFile(file, recursive));
    }
    return deleteFiles;
  }

  public Lease renewLease(String holder) throws IOException {
    return holder == null ? null : leaseRepository.insert(new Lease(holder, System.currentTimeMillis()), true);
  }

  public File findFileById(long id) throws IOException {
    File file = fileRepository.findById(id);
    if (file == null) return null;
    else if (file.isRootById()) {
      file.path = "";
      return file;
    } else {
      File parentFile = findFileById(file.parentId);
      if (parentFile == null) throw new IOException("fail to get parent file for " + file);
      file.path = parentFile.path + "/" + file.name;
      return file;
    }
  }

  public List<File> findFileByLeaseHolder(String leaseHolder) throws IOException {
    return fileRepository.findByLeaseHolder(leaseHolder);
  }

  public File findFileByBlockId(long blockId) throws IOException {
    List<Block> blocks = findBlockById(blockId);
    if (blocks == null || blocks.isEmpty() || blocks.get(0) == null) return null;
    return findFileById(blocks.get(0).fileId);
  }

  public List<File> findFilesOfExistedByPath(String path) throws IOException {
    File[] files = findFilesByPath(path);
    List<File> existedFiles = new ArrayList<File>(files.length);
    for (File file : files) {
      if (file == null) break;
      existedFiles.add(file);
    }
    return existedFiles;
  }

  public File[] findFilesByPath(String path) throws IOException {
    if (path == null) return new File[0];
    String[] names = Utilities.getNamesInPath(path);
    File[] files = new File[names.length];
    files[0] = fileRepository.findById(File.ROOT.id);
    files[0].path = "";
    for (int i = 1; i < names.length; ++i) {
      if (files[i - 1] == null) return files;
      files[i] = fileRepository.findByParentIdAndName(files[i - 1].id, names[i]);
      if (files[i] != null) files[i].path = files[i - 1].path + "/" + names[i];
    }
    files[0].path = "/";
    return files;
  }

  public File findFileByPath(String path) throws IOException {
    File[] files = findFilesByPath(path);
    return files.length == 0 ? null : files[files.length - 1];
  }

  public List<File> findFileChildrenByPath(String path) throws IOException {
    return findFileChildrenByFile(findFileByPath(path));
  }

  public List<File> findFileChildrenById(long id) throws IOException {
    return findFileChildrenByFile(findFileById(id));
  }

  public List<File> findFileChildrenByFile(File file) throws IOException {
    if (file == null) return null;
    else if (!file.isDir()) return new ArrayList<File>(0);
    else {
      List<File> childFiles = fileRepository.findByParentId(file.id);
      for (File childFile : childFiles) {
        if (file.path.equals("/")) childFile.path = "/" + childFile.name;
        else childFile.path = file.path + "/" + childFile.name;
      }
      return childFiles;
    }
  }

  public List<File> findFileDescendantByPath(String path, boolean excludeDir, boolean includeSelfAnyway)
      throws IOException {
    return findFileDescendantByFile(findFileByPath(path), excludeDir, includeSelfAnyway);
  }

  public List<File> findFileDescendantById(int id, boolean excludeDir, boolean includeSelfAnyway) throws IOException {
    return findFileDescendantByFile(findFileById(id), excludeDir, includeSelfAnyway);
  }

  public List<File> findFileDescendantByFile(File file, boolean excludeDir, boolean includeSelfAnyway)
      throws IOException {
    List<File> files = findFileDescendantByFileInternal(file, excludeDir);
    if (excludeDir && includeSelfAnyway) files.add(0, file);
    return files == null ? null : files;
  }

  public List<File> findFileDescendantByFileInternal(File file, boolean excludeDir) throws IOException {
    if (file == null) return null;
    List<File> files = new ArrayList<File>();
    if (!file.isDir()) {
      files.add(file);
      return files;
    }
    if (!excludeDir) files.add(file);

    List<File> childFiles = findFileChildrenByFile(file);
    if (childFiles == null) return files;
    for (File childFile : childFiles) {
      if (childFile == null) continue;
      if (!file.isDir()) files.add(childFile);
      files.addAll(findFileDescendantByFileInternal(childFile, excludeDir));
    }
    return files;
  }

  // for BlockProtocol
  public long countBlock() throws IOException {
    return blockRepository.count();
  }

  public List<Block> findBlockListById(long blockId) throws IOException {
    return blockRepository.findById(blockId);
  }

  public List<Block> findBlockById(long blockId) throws IOException {
    return blockRepository.findById(blockId);
  }

  public Block findBlockByIdAndDatanodeId(long id, long datanodeId) throws IOException {
    return blockRepository.findByIdAndDatanodeId(id, datanodeId);
  }

  private Map<Long, List<Block>> cacheForblockListOnDatandoe = new HashMap<Long, List<Block>>(10000);

  public List<Block> findBlockByDatanodeId(long datanodeId, boolean useCache) throws IOException {
    List<Block> blockListOnThisDatandoe = useCache ? cacheForblockListOnDatandoe.get(datanodeId) : null;
    if (blockListOnThisDatandoe == null) {
      synchronized (cacheForblockListOnDatandoe) {
        blockListOnThisDatandoe = blockRepository.findByDatanodeId(datanodeId);
        cacheForblockListOnDatandoe.put(datanodeId, blockListOnThisDatandoe);
      }
    }
    return blockListOnThisDatandoe;
  }

  public List<Block> findBlockAll() throws IOException {
    return blockRepository.findAll();
  }

  public Map<Long, Block> findBlockMapByDatanodeId(long datanodeId, boolean useCache) throws IOException {
    List<Block> blockListOnThisDatandoe = findBlockByDatanodeId(datanodeId, useCache);
    Map<Long, Block> blockMapOnThisDatanode = new HashMap<Long, Block>(blockListOnThisDatandoe.size());
    for (Block block : blockListOnThisDatandoe) {
      blockMapOnThisDatanode.put(block.id, block);
    }
    return blockMapOnThisDatanode;
  }

  public List<Block> findBlockByFilePath(String path) throws IOException {
    File file = findFileByPath(path);
    if (file == null) return null;
    return findBlockByFileId(file.id);
  }

  public List<Block> findBlockByFileId(long fileId) throws IOException {
    return blockRepository.findByFileId(fileId);
  }

  public List<Block> findBlockByFiles(List<File> fileList) throws IOException {
    if (fileList == null) return null;
    List<Block> blockList = new ArrayList<Block>();
    for (File file : fileList) {
      if (file == null || file.isDir()) continue;
      blockList.addAll(blockRepository.findByFileId(file.id));
    }
    return blockList;
  }

  public Block[] insertBlockByBlock(Block block) throws IOException {
    if (block == null) throw new IOException("block is null");
    return new Block[] { (Block) blockRepository.insert(block, false) };
  }

  public Block updateBlockByBlock(Block block, int fieldsIndication) throws IOException {
    return blockRepository.update(block, fieldsIndication);
  }

  public Block[] deleteBlockById(long id) throws IOException {
    List<Block> blocks = blockRepository.findById(id);
    List<Block> deletedBlocks = new ArrayList<Block>(blocks.size());
    for (Block block : blocks) {
      Block deletedBlock = (Block) blockRepository.delete(block);
      if (deletedBlock != null) deletedBlocks.add(deletedBlock);
    }
    return deletedBlocks.toArray(new Block[deletedBlocks.size()]);
  }

  public Block deleteBlockByIdAndDatanodeId(long id, long datanodeId) throws IOException {
    Block block = blockRepository.findByIdAndDatanodeId(id, datanodeId);
    return block == null ? null : blockRepository.delete(block);
  }

  public static HdfsFileStatus adfsFileToHdfsFileStatus(File file) {
    if (file == null) return null;
    return new HdfsFileStatus(file.length, file.isDir(), file.replication, file.blockSize, file.mtime, file.atime,
        null, null, null, file.path.getBytes());
  }

  public static HdfsFileStatus[] adfsFileArrayToHdfsFileStatusArray(List<File> fileList) {
    if (fileList == null) return null;
    HdfsFileStatus[] hdfsFileStatusArray = new HdfsFileStatus[fileList.size()];
    for (int i = 0; i < fileList.size(); ++i) {
      if (fileList.get(i) == null) continue;
      hdfsFileStatusArray[i] = adfsFileToHdfsFileStatus(fileList.get(i));
    }
    return hdfsFileStatusArray;
  }

  public static org.apache.hadoop.hdfs.protocol.Block adfsBlockToHdfsBlock(Block block) {
    return block == null ? null : new org.apache.hadoop.hdfs.protocol.Block(block.id, block.length,
        block.generationStamp);
  }

  public Lease findLeaseByHolder(String holder) throws IOException {
    return leaseRepository.findByHolder(holder);
  }

  public List<Lease> findLeaseByTimeLessThan(long time) throws IOException {
    return leaseRepository.findByTimeLessThan(time);
  }

  /**
   * insert or update old lease if exited
   */
  public Lease insertLeaseByHolder(String holder) throws IOException {
    return leaseRepository.insert(new Lease(holder, System.currentTimeMillis()), true);
  }

  public Lease deleteLeaseByHolder(String holder) throws IOException {
    return holder == null ? null : leaseRepository.delete(new Lease(holder, 0));
  }

  public Lease deleteLeaseByLease(Lease lease) throws IOException {
    return lease == null ? null : leaseRepository.delete(lease);
  }

  /** @return true if the Soft Limit Timer has expired */
  public static boolean expiredSoftLimit(long leaseTime) {
    return System.currentTimeMillis() - leaseTime > softLimit;
  }

  static public class LeaseMonitor implements Runnable {
    final String name = getClass().getSimpleName();

    public void run() {
     while(FSNamesystem.getFSNamesystem().isRunning()){
       try {
         long expiredHardLimitTime = System.currentTimeMillis() - hardLimit;
         List<Lease> leaseList = FSNamesystem.getFSStateManager().findLeaseByTimeLessThan(expiredHardLimitTime);
         for (Lease lease : leaseList) {
           List<File> fileList = FSNamesystem.getFSStateManager().findFileByLeaseHolder(lease.holder);
           for (File file : fileList) {
             FSNamesystem.getFSNamesystem().internalReleaseLeaseOne(file, lease.holder);
           }
           FSNamesystem.getFSStateManager().deleteLeaseByLease(lease);
         }
         Thread.sleep(2000);
       } catch (Throwable t) {
         FSNamesystem.LOG.error(t);
       }
     }
    }
  }
}
