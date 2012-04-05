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

 package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.file.File;
import com.taobao.adfs.state.StateManager;

public class DbNodeManager {
  
  private static final Log LOG = LogFactory.getLog(DbNodeManager.class);
  public static final String FILE_LOCK_PREFIX = "lockf-";
  private FSNamesystem fsNamesystem;

  DbNodeManager(FSNamesystem fsNamesystem) throws IOException {    
    this.fsNamesystem = fsNamesystem;
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_FILE_HOME, 
        new byte[0], false, true);
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_FILELOCK_HOME, 
        new byte[0], false, true);
    LOG.info("DbNodeManager initialized!");
  }
  
  public DbNodeInfo getDbNodeInfo(String src) throws IOException {
    File file = fsNamesystem.stateManager.getFileInfo(normalizePath(src));
    if(file != null) {
      return new DbNodeInfo(file.id, StateManager.fileToFileStatus(file));
    } else {
      return null;
    }   
  }
  
  public DbNodeFile getFile(int fileId) throws IOException{
    return getFile(fileId, getFileStatus(fileId));
  }
  
  public FileStatus getFileStatus(int fileId) throws IOException {
    File file = fsNamesystem.stateManager.getFileInfo(fileId);
    return StateManager.fileToFileStatus(file);
  }
  
  public DbNodeFile getFile(int fileId, FileStatus fs) throws IOException{
    if(fs != null && !fs.isDir()) {
      byte[] data = null;
      DbNodeFile file = null;
      if((data = getPendingData(fileId)) != null) {
        try {
          file = new DbNodePendingFile(fileId, fs, data, this);
        } catch(NullPointerException npe) {
          LOG.warn("getFile: NPE occurs when get file for file id="
              + fileId, npe);
          file = null; 
        }
      } else {
        file = new DbNodeFile(fileId, fs, this);
      }
      return file;
    } else {
      return null;
    }
  }
  
  public DbNodeInfo create(
      String src,
      boolean overwrite,
      byte replication,
      long blocksize) throws IOException {
    File file = fsNamesystem.stateManager.create(src, overwrite, 
        replication, (int)blocksize);
    return new DbNodeInfo(file.id, StateManager.fileToFileStatus(file));
  }
  
  public DbNodePendingFile addFile(
      int fileId,
      FileStatus fs,
      PermissionStatus perm, // ignore it
      String clientName,
      String clientMachine,
      int clientNodeId) throws IOException {
    if (fs != null) {
      String nodepath = getNodePath(fileId);
      DbNodePendingFile pendingFile = new DbNodePendingFile(fileId, fs, perm,
          clientName, clientMachine, clientNodeId, this);
      byte[] data = pendingFile.toByteArray();
      ZKClient.getInstance().create(nodepath, data, false, false);
      return pendingFile;
    } else {
      throw new IOException("addFile: can't get FileStatus using fileId="
          + fileId);
    }
  }
  
  public ZKClient.SimpleLock getFileLock(String src) throws IOException {
    String normal = normalizePath(src);
    return ZKClient.getInstance().getSimpleLock(
        FSConstants.ZOOKEEPER_FILELOCK_HOME, 
        normal.replace(DbNodePendingFile.SLASH, DbNodePendingFile.RSLASH));
  }
  
  public void updatePendingFileLength(DbNodePendingFile pendingFile) throws IOException {
    long len = pendingFile.computeContentSummary().getLength();
    fsNamesystem.stateManager.complete(pendingFile.fileId, len);
  }
  
  public void updatePendingFile(DbNodePendingFile pendingFile)
      throws IOException {
    byte[] data = pendingFile.toByteArray();
    ZKClient.getInstance().setData(getNodePath(pendingFile.getFileId()), data);
  }
  
  public void convertToDbNodeFile(DbNodePendingFile pendingFile) throws IOException {
    removePendingFile(pendingFile.getFileId());
    long length = pendingFile.computeContentSummary().getLength();
    if(LOG.isInfoEnabled()) {
      LOG.info("convertToDbNodeFile: src=" 
          + pendingFile + ", length=" + length);
    }
    fsNamesystem.stateManager.complete(pendingFile.fileId, length);
  }
  
  public void removePendingFile(int fileId) throws IOException {
    ZKClient.getInstance().delete(getNodePath(fileId), false);
  }
  
  public String getNodePath(int fileId) {
    StringBuilder nodePath = new StringBuilder(FSConstants.ZOOKEEPER_FILE_HOME);
    nodePath.append(DbNodePendingFile.SLASH).append(fileId);
    return nodePath.toString();
  }
  
  public ContentSummary getContentSummary(String src) throws IOException {
    String path= normalizePath(src);
    long summary[] = new long[] {0, 0, 0, 0};
    return computeContentSummary(path, summary);
  }
  
  public boolean isValidToCreate(String src, 
      DbNodeInfo dbNodeInfo) throws IOException {
    String srcs = normalizePath(src);
      if (srcs.startsWith(Path.SEPARATOR) && 
          !srcs.endsWith(Path.SEPARATOR) && 
          dbNodeInfo == null) {
        return true;
      } else {
        return false;
      }
  }

  String normalizePath(String src) {
    if(src != null) {
      if (src.length() > 1 && src.endsWith(Path.SEPARATOR)) {
        src = src.substring(0, src.length() - 1);
      }
    }
    return src;
  }
  
  private ContentSummary computeContentSummary(String src, long[] sum) throws IOException {
    File[] files = fsNamesystem.stateManager.getListing(src);
    FileStatus s;
    for(File file : files) {
      s = StateManager.fileToFileStatus(file);
      if(s.isDir()) {
        sum[2]++;
        computeContentSummary(s.getPath().toString(), sum);
      } else {
        sum[0] += s.getLen();
        sum[1]++;
        sum[3] += approximateDiskspaceConsumed(s);
      }
    }
    return new ContentSummary(sum[0], sum[1], sum[2], -1, sum[3], -1);
  }
  
  long approximateDiskspaceConsumed(FileStatus fs) {
    // for performance, we doesn't consider pending files calculation
    long approximateDiskspaceConsumed = 0;
    if(fs.getLen() != 0 && fs.getReplication() != 0) {
      long numOfFullBlocks = fs.getLen()/fs.getBlockSize();
      long lastBlockSize = fs.getLen()-numOfFullBlocks*fs.getBlockSize();
      approximateDiskspaceConsumed = 
        (numOfFullBlocks*fs.getBlockSize() + lastBlockSize)*fs.getReplication(); 
    }
    return approximateDiskspaceConsumed;
  }
  
  public byte[] getPendingData(int fileId) throws IOException {
    return ZKClient.getInstance().getData(getNodePath(fileId), null);
  }
  
  public BlockInfo[] getBlocks(DbNodeFile file) throws IOException {
    BlockInfo[] blocks = null;
    Map<Long, BlockInfo> blockMap = new LinkedHashMap<Long, BlockInfo>();
    List<BlockEntry> fileBlocksList = 
      fsNamesystem.stateManager.getBlocksByFileID(file.getFileId());
    if(fileBlocksList != null) {
      for (int i = 0; i < fileBlocksList.size(); i++) {
        BlockEntry entry = fileBlocksList.get(i);
        if(blockMap.containsKey(entry.getBlockId())) {
          // this is the abnormal situation 
          LOG.warn("getBlocks: hey, blockmap returns a list containing more than one element of " 
              + entry.getBlockId() + ", WHY???...Well, try to fix it");
          BlockInfo storedBI = blockMap.get(entry.getBlockId());
          if (storedBI.getGenerationStamp() > entry.getGenerationStamp()) {
            // entry is older date element, skip...
            continue;
          } else if (storedBI.getGenerationStamp() == entry.getGenerationStamp()) {
            if(entry.getNumbytes() > storedBI.getNumBytes()) {
              // replace the larger value
              storedBI.setNumBytes(entry.getNumbytes());
            }
            DatanodeDescriptor[] added = getDatanodes(entry.getDatanodeIds());
            if(added != null) {
              for(DatanodeDescriptor dn : added) {
                storedBI.addNode(dn);
              }
            }
            // completed the fix!
            continue;
          }
        }
        Block b = new Block(entry.getBlockId(), 
            entry.getNumbytes(), entry.getGenerationStamp());
        DatanodeDescriptor[] dnDescArray = getDatanodes(entry.getDatanodeIds());
        BlockInfo blkInfo = new BlockInfo(b, dnDescArray.length);
        blkInfo.setDataNode(dnDescArray);
        if(LOG.isInfoEnabled()) {
          LOG.info("getBlocks: id=" + entry.getBlockId() 
              + ", numbytes=" + entry.getNumbytes() + ", genstamp=" + entry.getGenerationStamp()
              + ", nodeIds=" + entry.getDatanodeIds()
              + ", dnDescNums=" + blkInfo.getDatanode().length 
              + ", file=" + file);
        }
        blockMap.put(entry.getBlockId(), blkInfo);
      }
    }
    
    if(blockMap.values() != null && blockMap.values().size() > 0) {
      blocks = blockMap.values().toArray(new BlockInfo[0]);
    } else {
      blocks = new BlockInfo[0];
      if(LOG.isInfoEnabled()) {
        LOG.warn("getBlocks: no block is found for file = " + file);
      }
    }
    return blocks; 
  }
  
  private DatanodeDescriptor[] getDatanodes(List<Integer> datanodeIds) {
    List<DatanodeDescriptor> targetList = new ArrayList<DatanodeDescriptor>();
    if (datanodeIds != null && datanodeIds.size() > 0) {
      for (Iterator<Integer> it = datanodeIds.iterator(); it.hasNext();) {
        DatanodeDescriptor dnDesc = 
          fsNamesystem.getDataNodeDescriptorByID(it.next());
        if (dnDesc != null) {
          targetList.add(dnDesc);
        }
      }
    }
    return targetList.toArray(new DatanodeDescriptor[0]);
  }
  
  DatanodeDescriptor getDatanode(int nodeId) {
    DatanodeDescriptor dn = fsNamesystem.getDataNodeDescriptorByID(nodeId);
    return dn;
  }
  
  DatanodeDescriptor[] getTargets(Set<Integer> targets) {
    // allocate new data structure to store additional target
    DatanodeDescriptor[] newt = new DatanodeDescriptor[targets.size()];
    int count = 0;
    for (Integer target : targets) {
      newt[count++] = fsNamesystem.getDataNodeDescriptorByID(target);
    }
    return newt;
  }
}
