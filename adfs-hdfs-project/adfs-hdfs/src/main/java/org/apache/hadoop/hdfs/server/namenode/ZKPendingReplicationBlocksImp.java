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
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.zookeeper.data.Stat;

public class ZKPendingReplicationBlocksImp implements AbsPendingReplicationBlocks, FSConstants{

  private static final Log LOG = LogFactory.getLog(ZKPendingReplicationBlocksImp.class);
  private static final String MONITOR_LOCK_PREFIX = "lockm-";
  private static final String SLASH = "/";
  private static final Charset CHARSET = Charset.forName("UTF-8");

  Daemon timerThread = null;
  private volatile boolean fsRunning = true;
  private final DbLockManager lockManager;
 
  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  private long timeout = 5 * 60 * 1000;
  private long defaultRecheckInterval = 5 * 60 * 1000;
  
  private ArrayList<Block> timedOutItems;
  
  ZKPendingReplicationBlocksImp(DbLockManager locker, long timeoutPeriod) throws IOException {
    lockManager = locker;
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    init();
  }
  
  private void init() throws IOException {
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_PENDING_HOME, 
        new byte[0], false, true);
    timedOutItems = new ArrayList<Block>();
    this.timerThread = new Daemon(new PendingReplicationMonitor());
    timerThread.start();
  }

  @Override
  public void add(Block block, int numReplicas) throws IOException {
    String path = FSConstants.ZOOKEEPER_PENDING_HOME + SLASH
        + block.getBlockId();
    ZkPendingBlockInfo zkBlkInfo = new ZkPendingBlockInfo();
    byte[] olddata = ZKClient.getInstance().getData(path, null);
    if (olddata != null) {
      zkBlkInfo.set(olddata);
      if (zkBlkInfo.removed) {
        // in case it is a previous block to be removed
        ZKClient.getInstance().delete(path, false);
        zkBlkInfo.set(block, numReplicas);
        ZKClient.getInstance().create(path, 
            zkBlkInfo.toByteArray(), false, false);
      } else {
        zkBlkInfo.adjustReplicas(numReplicas);
        ZKClient.getInstance().setData(path, zkBlkInfo.toByteArray());
      }
    } else {
      zkBlkInfo.set(block, numReplicas);
      ZKClient.getInstance().create(path, 
          zkBlkInfo.toByteArray(), false, false);
    }

  }

  @Override
  public int getNumReplicas(Block block) throws IOException {
    String path = FSConstants.ZOOKEEPER_PENDING_HOME + SLASH
        + block.getBlockId();
    byte[] data = ZKClient.getInstance().getData(path, null);
    if (data != null) {
      ZkPendingBlockInfo zkBlkInfo = new ZkPendingBlockInfo(data);
      // we will check if it is removed actually
      if (!zkBlkInfo.removed) {
        return zkBlkInfo.numReplicasInProgress;
      } else {
        return 0;
      }
    } else {
      return 0;
    }
  }

  @Override
  public void remove(Block block) throws IOException {
    String path = FSConstants.ZOOKEEPER_PENDING_HOME + SLASH
        + block.getBlockId();
    byte[] olddata = ZKClient.getInstance().getData(path, null);
    if (olddata != null) {
      ZkPendingBlockInfo zkBlkInfo = new ZkPendingBlockInfo(olddata);
      if (zkBlkInfo.numReplicasInProgress - 1 > 0 && !zkBlkInfo.removed) {
        zkBlkInfo.adjustReplicas(-1);
        ZKClient.getInstance().setData(path, zkBlkInfo.toByteArray());
      } else {
        ZKClient.getInstance().delete(path, false);
      }
    }
  }

  @Override
  public int size() throws IOException{    
    String path;
    byte[] data;
    int count = 0;
    // lockless check for all pending nodes
    List<String> coll = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_PENDING_HOME, null);
    if(coll == null) return count;
    for(String one : coll) { // filter locks..
      if(one.startsWith(MONITOR_LOCK_PREFIX))
        continue;
      path = FSConstants.ZOOKEEPER_PENDING_HOME + SLASH + one;
      data = ZKClient.getInstance().getData(path, null);
      if(data != null) {
        ZkPendingBlockInfo zkBlkInfo = new ZkPendingBlockInfo(data);
        if(!zkBlkInfo.removed) count++;
      }
    }
    return count;
  }

  @Override
  public void stop(){
    fsRunning = false;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }
  
  @Override
  public Block[] getTimedOutBlocks() throws IOException {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      Block[] blockList = timedOutItems
          .toArray(new Block[timedOutItems.size()]);
      timedOutItems.clear();
      return blockList;
    }
  }

  @Override
  public void metaSave(PrintWriter out) throws IOException{
    throw new UnsupportedOperationException("metaSave");
  }
  
  private static class ZkPendingBlockInfo {
    
    private static final String SEPARATOR = ";";
    private int numReplicasInProgress;
    private long numBytes;
    private long generationStamp;
    /**
     * add this flag for getNumReplicas query to indicate this block
     * is removed due to timeout, but it still 
     * exists physically to avoid data missing in case of 
     * namenode crashed. It will be remove physically when 
     * future remove operation
     */
    public boolean removed;
    
    private ZkPendingBlockInfo() {
      numReplicasInProgress = 0;
      numBytes = 0;
      generationStamp = 0;
      removed = false;
    }
    
    private ZkPendingBlockInfo(byte[] data) {
      set(data);
    }
    
    private ZkPendingBlockInfo(Block block, int numReplicas) {
      set(block, numReplicas);
    }
    
    private void set(byte[] data) {
      String strData = new String(data, CHARSET);
      String[] datas = strData.split(SEPARATOR);
      numReplicasInProgress = Integer.parseInt(datas[0]);
      numBytes = Long.parseLong(datas[1]);
      generationStamp = Long.parseLong(datas[2]);
      removed = Boolean.parseBoolean(datas[3]);
      strData = null;
    }
    
    private void set(Block block, int numReplicas) {
      this.numReplicasInProgress = numReplicas;
      this.numBytes = block.getNumBytes();
      this.generationStamp = block.getGenerationStamp();
      removed = false;
    }
    
    private synchronized void adjustReplicas(int delta) {
      numReplicasInProgress += delta;
      assert(numReplicasInProgress >= 0);
    }
    
    private synchronized void setRemoved() {
      removed = true;
    }
    
    private byte[] toByteArray() {
      StringBuilder sb = new StringBuilder();
      sb.append(numReplicasInProgress)
        .append(SEPARATOR).append(numBytes)
        .append(SEPARATOR).append(generationStamp)
        .append(SEPARATOR).append(removed);
      return sb.toString().getBytes(CHARSET);
    }
    
  }

  /*
   * A periodic thread that scans for blocks that never finished
   * their replication request.
   */
  class PendingReplicationMonitor implements Runnable {
    public void run() {
      while (fsRunning) {
        long period = Math.min(defaultRecheckInterval, timeout);
        try {
          // the whole system is only allowed one PendingReplicationMonitor
          if(lockManager.trylockPendingMonitor()) {
            if(LOG.isDebugEnabled()) {
              LOG.debug("running the PendingReplicationMonitor");
            }
            long start = now();
            try {
              pendingReplicationCheck();
            } finally {
              lockManager.unlockPendingMonitor();
            }
            if(LOG.isDebugEnabled()) {
              LOG.debug("dropped the PendingReplicationMonitor lock, " +
              		"holding it for " + (now() - start) + " msec");
            }
          } else {
            LOG.debug("another name node is holding replication monitor lock");
          }
          Thread.sleep(period);
        } catch (Exception ie) {
          LOG.warn(
                "PendingReplicationMonitor thread received exception. " + ie);
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReplicationCheck() throws IOException {

      List<String> children = 
        ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_PENDING_HOME, null);
      if(children == null) return;
      Iterator<String> iter = children.iterator();
      LOG.debug("PendingReplicationMonitor checking Q");
      String name = null;
      String child;
      Stat stat = null;
      Block block = new Block(); // reuse
      ZkPendingBlockInfo zkBlkInfo = new ZkPendingBlockInfo(); // reuse
      while (iter.hasNext()) {
        name = iter.next();
        if (name.startsWith(MONITOR_LOCK_PREFIX))
          continue;
        child = FSConstants.ZOOKEEPER_PENDING_HOME + SLASH + name;
        stat = ZKClient.getInstance().exist(child);
        if (stat != null && now() > stat.getMtime() + timeout) {
          byte[] data = ZKClient.getInstance().getData(child, null);
          if (data == null)
            continue;
          zkBlkInfo.set(data);
          block.set(Long.parseLong(name), zkBlkInfo.numBytes,
              zkBlkInfo.generationStamp);
          if (!zkBlkInfo.removed) {
            // we set it removed, but not delete it here
            zkBlkInfo.setRemoved();
            ZKClient.getInstance().setData(child, zkBlkInfo.toByteArray());
          }
          synchronized (timedOutItems) {
            if (!timedOutItems.contains(block)) {
              timedOutItems.add(new Block(block));
              LOG.warn("PendingReplicationMonitor timed out block " + block);
            }
          }
        } // end of time-out check
      }
      children.clear();
    }
  }
  
  long now() {
    return System.currentTimeMillis();
  }

}
