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

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.zookeeper.data.Stat;

public class ZKRecentInvalidateSetsImp implements AbsRecentInvalidateSets,
    FSConstants {

  private static final Log LOG = LogFactory
      .getLog(ZKRecentInvalidateSetsImp.class);
  private static final String SLASH = "/";
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private long timeout;
  private long heartbeatInterval;
  private long blockReportInterval;
  private final FSNamesystem namesystem;
  private final DbLockManager lockManager;
  Daemon removedMonitor = null;
  private volatile boolean fsRunning = true;

  
  public ZKRecentInvalidateSetsImp(FSNamesystem namesystem,
      long heartbeatInterval, long blockReportInterval) throws IOException {
    this.namesystem = namesystem;
    this.lockManager = namesystem.lockManager;
    this.heartbeatInterval = heartbeatInterval;
    this.blockReportInterval = blockReportInterval;
 
    this.timeout = blockReportInterval + 2 * heartbeatInterval;
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_INVALIDATE_HOME,
        new byte[0], false, true);
    this.removedMonitor = new Daemon(new RemovedBlockTimeoutMonitor());
    removedMonitor.start();
  }

  /** put the storageID and block pair to this set,
   *  if there is block in such storageID, 
   *  mark the storageID Znode's data as '0' */
  public boolean put(String storageID, Block block) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID);
    // this means blocks are available (not 'removed') under this storageID
    byte[] data = { 0 };
    // it doesn't matter if the node exists
    ZKClient.getInstance().create(sb.toString(), data, false, false);
    ZKClient.getInstance().setData(sb.toString(), data);
    ZkInvalidateBlockInfo blockInfo = new ZkInvalidateBlockInfo(block);
    sb.append(SLASH).append(block.getBlockId());
    ZKClient.getInstance().create(sb.toString(), 
        blockInfo.toByteArray(), false, false);
    return true;
  }

  /** get FirstNodeId in recentInvalidate,the storageID dir should have block
      unseted to removed */
  public String getFirstNodeId() throws IOException {
    String firstNode = null;
    List<String> storageIDList = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_INVALIDATE_HOME, null);
    if (storageIDList != null && storageIDList.size() > 0) {
      for (String storageID : storageIDList) {
        if (storageID == null)
          continue;
        StringBuilder sb = new StringBuilder(
            FSConstants.ZOOKEEPER_INVALIDATE_HOME);
        sb.append(SLASH).append(storageID);
        byte[] storageData = null;
        storageData = ZKClient.getInstance().getData(sb.toString(), null);
        if (storageData != null && storageData[0] == 0) {
          // this storage has available blocks
          firstNode = storageID;
          break;
        }
      }
    }
    return firstNode;
  }

  /** get the first node's blocks in invalidateSets */
  public ArrayList<Block> getBlocks(String storageID, int blockLimit)
      throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID);
    ArrayList<Block> blocksToInvalidate = new ArrayList<Block>(blockLimit);
    byte[] storageData = ZKClient.getInstance().getData(sb.toString(), null);
    // need to re-check here
    if (storageData != null && storageData[0] == 0) {
      Iterator<String> it = getBlockIterator(storageID);
      if (it != null) {
        for (int blkCount = 0; blkCount < blockLimit && it.hasNext();) {
          Block block = get(storageID, it.next());
          if (block != null) {
            blocksToInvalidate.add(block);
            blkCount++;
          }
        }
        /*
         * if all the block has been get and setRemoved, set parent data as
         * 'removed'
         */
        if (!it.hasNext()) {
          byte[] data = { 1 };
          ZKClient.getInstance().setData(sb.toString(), data);
        }
      }
    }
    return blocksToInvalidate;
  }
  
  /** remove blocks by storageID without confirmation */
  public int remove(String storageID) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID);
    int blockCount = 0;
    Iterator<String> iter = getBlockIterator(storageID);
    if (iter != null) {
      for (; iter.hasNext();) {
        removeInternal(storageID, iter.next());
        blockCount++;
      }
    } else {
      ZKClient.getInstance().delete(sb.toString(), false);
      blockCount = 0;
    }

    return blockCount;
  }

  /** remove a block from this set without confirmation */
  public int remove(long blockID) throws IOException {
    int blockCount = 0;
    String strBlkID = String.valueOf(blockID);
    List<String> storageIDList = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_INVALIDATE_HOME, null);
    for (String storageID : storageIDList) {
      if (storageID == null)
        continue;
      if (removeInternal(storageID, strBlkID)) {
        blockCount++;
      }
    }
    return blockCount;
  }
  
  /** remove blockID znode physically, if storageID directory in ZK is empty, delete it */
  public boolean remove(String storageID, String blockID) throws IOException {
    boolean ret = false;
    ret = removeInternal(storageID, blockID);
    return ret;
  }

  private boolean removeInternal(String storageID, String blockID) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID).append(SLASH).append(blockID);
    return ZKClient.getInstance().delete(sb.toString(), true, true);
  }

  public void stop() {
    fsRunning = false;
    removedMonitor.interrupt();
    try {
      removedMonitor.join(3000);
    } catch (InterruptedException ie) {
    }
  }
  
  /** get block data from ZK and mark it as 'removed', update block data in ZK */
  private Block get(String storageID, String blockID) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID).append(SLASH).append(blockID);
    byte[] blockBytes = ZKClient.getInstance().getData(sb.toString(), null);
    if (blockBytes != null) {
      ZkInvalidateBlockInfo blockInfo = 
        new ZkInvalidateBlockInfo(blockBytes);
      if (!blockInfo.isRemoved()) { // block is not set removed
        Block b = new Block(Long.parseLong(blockID), blockInfo.getNumBytes(),
            blockInfo.getGenStamp());
        blockInfo.setRemoved(true); // set block removed
        ZKClient.getInstance().setData(sb.toString(), blockInfo.toByteArray());
        return b;
      } else { // block is already removed
        return null;
      }
    } else {
      return null;
    }
  }

  private Iterator<String> getBlockIterator(String storageID)
      throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_INVALIDATE_HOME);
    sb.append(SLASH).append(storageID);
    List<String> blockList = ZKClient.getInstance().getChildren(sb.toString(),
        null);
    if (blockList != null)
      return blockList.iterator();
    else
      return null;
  }

  class RemovedBlockTimeoutMonitor implements Runnable {

    @Override
    public void run() {
      while (fsRunning) {
        try {
          Thread.sleep(2 * heartbeatInterval);
          if (lockManager.trylockInvalidateMonitor()) {
            try {
              checkRemovedBlockTimeout();
            } finally {
              lockManager.unlockInvalidateMonitor();
            }
          }
          Thread.sleep(blockReportInterval);
        } catch (IOException ioe) {
          LOG.warn("RemovedBlockTimeoutMonitor thread " +
          		"throws IOException", ioe);
        } catch (Exception e) {
          LOG.warn("RemovedBlockTimeoutMonitor thread " +
          		"throws Exception", e);
        }
      }// end of while
    }

    private void checkRemovedBlockTimeout() throws IOException {
      List<String> storageIDList = ZKClient.getInstance().getChildren(
          FSConstants.ZOOKEEPER_INVALIDATE_HOME, null);
      for (String storageID : storageIDList) {
        if(storageID == null)
          continue;
        checkRemoveBlockOnOneDatanode(storageID);
      }
    }
    
    private void checkRemoveBlockOnOneDatanode(String storageID) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<String> it = getBlockIterator(storageID);
      if (it == null)
        return;
      for (;it.hasNext();) {
        String blockID = it.next();
        sb.delete(0, sb.length());
        sb.append(FSConstants.ZOOKEEPER_INVALIDATE_HOME).append(SLASH)
            .append(storageID).append(SLASH).append(blockID);
        Stat stat = ZKClient.getInstance().exist(sb.toString());
        byte[] blockBytes = ZKClient.getInstance().getData(sb.toString(),
            null);
        if (blockBytes != null) {
          ZkInvalidateBlockInfo blockInfo = new ZkInvalidateBlockInfo(
              blockBytes);
          // block is set removed and timeout
          if (blockInfo.isRemoved() && now() > stat.getMtime() + timeout) {
            long blockId = Long.parseLong(blockID);
            if (namesystem.containsBlock(storageID, blockId)) { // block is
              // still in blockMap(DB), ask ReplicationMonitor to handle
              blockInfo.setRemoved(false);
              ZKClient.getInstance().setData(sb.toString(),
                  blockInfo.toByteArray());
              // reset to 0 -- datanode has available blocks
              byte[] data = {0};
              ZKClient.getInstance().setData(FSConstants.ZOOKEEPER_INVALIDATE_HOME
                  + SLASH + storageID, data);
              LOG.info("block " + blockID + " at " + storageID
                  + " is time-out to be removed");
            } else {
              sb.delete(0, sb.length());
              sb.append(FSConstants.ZOOKEEPER_EXCESS_HOME).append(SLASH).append(storageID).append(SLASH)
                  .append(blockID);
              ZKClient.getInstance().delete(sb.toString(), true);
              sb.delete(0, sb.length());
              sb.append(FSConstants.ZOOKEEPER_CORRUPT_HOME).append(SLASH).append(blockID).append(SLASH)
                  .append(storageID);
              ZKClient.getInstance().delete(sb.toString(), true);
              sb.delete(0, sb.length());
              sb.append(FSConstants.ZOOKEEPER_INVALIDATE_HOME).append(SLASH)
                  .append(storageID).append(SLASH).append(blockID);
              ZKClient.getInstance().delete(sb.toString(), true);
              LOG.info("block " + blockID + " at " + storageID
                  + " doesn't exist, so remove it");
            }
          }
        } // end of blockBytes != null
      } // end of loop
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }

  static class ZkInvalidateBlockInfo {

    private static final String SEPARATOR = ";";
    private long numBytes;
    private long generationStamp;
    private boolean removed = false;

    private ZkInvalidateBlockInfo(byte[] data) {
      String strData = new String(data, CHARSET);
      String[] datas = strData.split(SEPARATOR);
      numBytes = Long.parseLong(datas[0]);
      generationStamp = Long.parseLong(datas[1]);
      removed = Boolean.parseBoolean(datas[2]);
    }

    private ZkInvalidateBlockInfo(Block block) {
      numBytes = block.getNumBytes();
      generationStamp = block.getGenerationStamp();
      removed = false;
    }

    private byte[] toByteArray() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.numBytes).append(SEPARATOR).append(this.generationStamp)
          .append(SEPARATOR).append(this.removed);
      return sb.toString().getBytes(CHARSET);
    }

    public final boolean isRemoved() {
      return removed;
    }

    public final long getNumBytes() {
      return numBytes;
    }

    public final long getGenStamp() {
      return generationStamp;
    }

    public void setRemoved(boolean val) {
      removed = val;
    }
  }

}
