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
import java.nio.charset.Charset;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;

public class DbNodePendingFile extends DbNodeFile {

  private static final Charset CHARSET = Charset.forName("UTF-8");
  public static final String SLASH = "/";
  public static final String RSLASH = "\\";
  public static final String SEPARATOR = "\n";
  private static final String COMMA = ",";
  private static final String PLACEHOLDER = "?";
  
  private String clientName = null;     // lease holder
  private String clientMachine;
  private int clientDatanodeId = -1;  // if client is a cluster node too.
  
  private int primaryNodeIndex = -1;    // the node working on lease recovery
  private SortedSet<Integer> targetDatanodeIds = null;   // locations for last block
  private long lastRecoveryTime = 0;
  
  public DbNodePendingFile(
      int fileId,
      FileStatus fs,
      PermissionStatus perm, // ignore it
      String clientName,
      String clientMachine,
      int clientNodeId,
      DbNodeManager manager) throws IOException {
    super(fileId, fs, manager);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientDatanodeId = clientNodeId;
    this.targetDatanodeIds = new TreeSet<Integer>();
  }
  
  public DbNodePendingFile(int fileId, FileStatus fs, 
      byte[] data, DbNodeManager manager) throws IOException {
    super(fileId, fs, manager);
    this.targetDatanodeIds = new TreeSet<Integer>();
    zkLoad(data);
  }
  
  private void zkLoad(byte[] data) throws IOException {
    String strData = new String(data, CHARSET);
    String[] datas = strData.split(SEPARATOR);
    clientName = datas[0];
    clientMachine = datas[1];
    clientDatanodeId = Integer.parseInt(datas[2]);
    lastRecoveryTime = Long.parseLong(datas[3]);
    primaryNodeIndex = Integer.parseInt(datas[4]);
    String strTargets = datas[5];
    if(!PLACEHOLDER.equals(strTargets)) {
      String[] targets = strTargets.split(COMMA);
      for(String target : targets) {
        targetDatanodeIds.add(Integer.parseInt(target));
      }
    }
  }
  
  public byte[] toByteArray() {
    StringBuilder sb = new StringBuilder();
    sb.append(clientName)
      .append(SEPARATOR).append(clientMachine)
      .append(SEPARATOR).append(clientDatanodeId)
      .append(SEPARATOR).append(lastRecoveryTime)
      .append(SEPARATOR).append(primaryNodeIndex)
      .append(SEPARATOR);
    int last = targetDatanodeIds.size()-1;
    if(last >= 0) {
      for(int i : targetDatanodeIds) {
        sb.append(i);
        if(last-- > 0) {
          sb.append(COMMA);
        }
      }
    }else{
      sb.append(PLACEHOLDER);
    }
    return sb.toString().getBytes(CHARSET);
  }
  
  String getClientName() {
    return clientName;
  }

  void setClientName(String newName) throws IOException {
    clientName = newName;
    dbFileManager.updatePendingFile(this);
  }

  String getClientMachine() throws IOException {
    return clientMachine;
  }

  DatanodeDescriptor getClientNode() {
    return dbFileManager.getDatanode(clientDatanodeId);
  }
  
  DatanodeDescriptor[] getTargets() {
    return dbFileManager.getTargets(targetDatanodeIds);
  }
  
  void setTargets(DatanodeDescriptor[] targets) throws IOException {
    setTargetsInternal(targets);
    dbFileManager.updatePendingFile(this);
  }
  
  private void setTargetsInternal(DatanodeDescriptor[] targets) {
    if(targets != null) {
      for(DatanodeDescriptor target : targets) {
        targetDatanodeIds.add(target.getNodeid());
      }
    }
    this.primaryNodeIndex = -1;
  }

  void addTarget(DatanodeDescriptor node) throws IOException {
    targetDatanodeIds.add(node.getNodeid());
    this.primaryNodeIndex = -1;
    dbFileManager.updatePendingFile(this);
  }
  
  void updateLength() throws IOException {
    dbFileManager.updatePendingFileLength(this);
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  public boolean isUnderConstruction() {
    return true;
  }
  
  /**
   * remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeBlock(Block oldblock) throws IOException {
    lazyLoadBlocks();

    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }

    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    blocks = newlist;
    
    // Remove the block locations for the last block.
    targetDatanodeIds.clear();
    // Commit to ZooKeeper
    dbFileManager.updatePendingFile(this);
  }
  
  void setLastBlock(BlockInfo newblock,
      DatanodeDescriptor[] newtargets) throws IOException {
    lazyLoadBlocks();
    
    if (blocks == null || blocks.length == 0) {
      throw new IOException("Trying to update non-existant block (newblock="
          + newblock + ")");
    }
    
    Block oldLast = blocks[blocks.length - 1];
    if (oldLast.getBlockId() != newblock.getBlockId()) {
      // This should not happen - this means that we're performing recovery
      // on an internal block in the file!
      NameNode.stateChangeLog
          .error("Trying to commit block synchronization for an internal block on"
              + " inode="
              + this
              + " newblock="
              + newblock
              + " oldLast="
              + oldLast);
      throw new IOException("Trying to update an internal block of "
          + "pending file " + this);
    }

    if (oldLast.getGenerationStamp() > newblock.getGenerationStamp()) {
      NameNode.stateChangeLog.warn("Updating last block " + oldLast
          + " of inode " + "under construction " + this + " with a block that "
          + "has an older generation stamp: " + newblock);
    }

    blocks[blocks.length - 1] = newblock;
    setTargetsInternal(newtargets);
    lastRecoveryTime = 0;
    
    // Commit to ZooKeeper
    dbFileManager.updatePendingFile(this);
  }
  
  /**
   * Initialize lease recovery for this object
   * @throws IOException 
   */
  DatanodeDescriptor assignPrimaryDatanode() throws IOException {
    //assign the first alive datanode as the primary datanode
    if (targetDatanodeIds.size() == 0) {
      NameNode.stateChangeLog.warn("BLOCK*"
        + " INodeFileUnderConstruction.initLeaseRecovery:"
        + " No blocks found, lease removed.");
    }

    int previous = primaryNodeIndex;
    DatanodeDescriptor[] targets = getTargets();
    BlockInfo[] blocks = getBlocks();
    // find an alive datanode beginning from previous.
    // This causes us to cycle through the targets on successive retries.
    if(blocks != null && blocks.length > 0) {
      for(int i = 1; i <= targets.length; i++) {
        int j = (previous + i)%targets.length;
        if (targets[j] != null && targets[j].isAlive) {
          DatanodeDescriptor primary = targets[primaryNodeIndex = j]; 
          primary.addBlockToBeRecovered(blocks[blocks.length - 1], targets);
          NameNode.stateChangeLog.info("BLOCK* " + blocks[blocks.length - 1]
            + " recovery started, primary=" + primary);
          // Commit to ZooKeeper
          dbFileManager.updatePendingFile(this);
          return primary; // we need this handler to further process
        }
      }
    }
    return null;
  }
  
  /**
   * Update lastRecoveryTime if expired.
   * @return true if lastRecoveryTime is updated. 
   * @throws IOException 
   */
  boolean setLastRecoveryTime(long now) throws IOException {
    boolean expired = now - lastRecoveryTime > NameNode.LEASE_RECOVER_PERIOD;
    if (expired) {
      lastRecoveryTime = now;
      // Commit to ZooKeeper
      dbFileManager.updatePendingFile(this);
    }
    return expired;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("[FILE:id=");
    sb.append(fileId)
      .append("|path=")
      .append(filePath)
      .append("|holder=")
      .append(clientName)
      .append("]");
    return sb.toString();
  }

}
