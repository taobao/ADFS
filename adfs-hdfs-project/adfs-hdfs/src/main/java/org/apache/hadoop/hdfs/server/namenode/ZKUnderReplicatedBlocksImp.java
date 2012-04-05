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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.data.Stat;

public class ZKUnderReplicatedBlocksImp implements AbsUnderReplicatedBlocks, FSConstants{
  
  private static final Log LOG = LogFactory.getLog(ZKUnderReplicatedBlocksImp.class);
  private static final String SLASH = "/";
  private static final String PLACEHOLDER = "?";
  private static final Charset CHARSET = Charset.forName("UTF-8");
  
  
  public ZKUnderReplicatedBlocksImp() throws IOException{
    for(int i=0;i<3;i++){
      ZKClient.getInstance().create(FSConstants.ZOOKEEPER_UNDER_HOME+"/"+i, 
          new byte[0], true, true);
    }
    LOG.info("ZKUnderReplicatedBlocksImp is initialized!");
  }
  /** add a block to a under replication queue according to its priority
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  @Override
  public boolean add(Block block, int curReplicas, int decomissionedReplicas,
      int expectedReplicas) throws IOException {
    if(curReplicas<0 || expectedReplicas<=curReplicas) {
      return false;
    }
    int priLevel = getPriority(block, curReplicas, decomissionedReplicas,
                               expectedReplicas);
    boolean ret = add(block, priLevel);
    if(ret) {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.UnderReplicationBlock.add:"
          + block
          + " has only "+curReplicas
          + " replicas and need " + expectedReplicas
          + " replicas so is added to neededReplications"
          + " at priority level " + priLevel);
    }
    return ret;
  }

  private boolean add(Block block, int priLevel) throws IOException {
    if (priLevel != LEVEL) {
      StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_UNDER_HOME);
      sb.append(SLASH).append(priLevel).append(SLASH)
          .append(block.getBlockId());
      ZkUnderBlockInfo blkInfo = new ZkUnderBlockInfo(block);
      ZKClient.getInstance()
          .create(sb.toString(), blkInfo.toByteArray(), false, false);
      return true;
    }
    return false;
  }

  /** Check if a block is in the neededReplication queue */
  @Override
  public boolean contains(Block block) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_UNDER_HOME);
    sb.append(SLASH).append(PLACEHOLDER);
    int mark = sb.length() - 1;
    sb.append(SLASH).append(block.getBlockId());
    Stat stat;
    for (int i = 0; i < LEVEL; i++) {
      sb.replace(mark, mark + 1, String.valueOf(i));
      stat = ZKClient.getInstance().exist(sb.toString());
      if (stat != null) {
        return true;
      }
    }
    return false;
  }

  /** remove a block from a under replication queue */
  @Override
  public boolean remove(Block block, int oldReplicas,
      int decommissionedReplicas, int oldExpectedReplicas) throws IOException{
    int priLevel = getPriority(block, oldReplicas, 
        decommissionedReplicas,
        oldExpectedReplicas);
    return remove(block, priLevel);
  }

  /** remove a block from a under replication queue given a priority*/
  @Override
  public boolean remove(Block block, int priLevel) throws IOException{
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_UNDER_HOME);
    sb.append(SLASH).append(PLACEHOLDER);
    int mark = sb.length()-1;
    sb.append(SLASH).append(block.getBlockId());
    Stat stat;
    sb.replace(mark, mark + 1, String.valueOf(priLevel));
    stat = ZKClient.getInstance().exist(sb.toString());
    if (priLevel >= 0 && priLevel < LEVEL && stat != null) {
      ZKClient.getInstance().delete(sb.toString(), false);
      NameNode.stateChangeLog
          .debug("BLOCK* NameSystem.UnderReplicationBlock.remove: "
              + "Removing block " + block + " from priority queue " + priLevel);
    } else {
      for (int i = 0; i < LEVEL; i++) {
        if (i != priLevel) {
          sb.replace(mark, mark + 1, String.valueOf(i));
          ZKClient.getInstance().delete(sb.toString(), false);
        }
      }
    }
    return true;
  }

  /** update the priority level of a block */
  @Override
  public void update(Block block, int curReplicas, int decommissionedReplicas,
      int curExpectedReplicas, int curReplicasDelta, int expectedReplicasDelta) throws IOException{
    int oldReplicas = curReplicas-curReplicasDelta;
    int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
    int curPri = getPriority(block, curReplicas, decommissionedReplicas, curExpectedReplicas);
    int oldPri = getPriority(block, oldReplicas, decommissionedReplicas, oldExpectedReplicas);
    NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
                                  block +
                                  " curReplicas " + curReplicas +
                                  " curExpectedReplicas " + curExpectedReplicas +
                                  " oldReplicas " + oldReplicas +
                                  " oldExpectedReplicas  " + oldExpectedReplicas +
                                  " curPri  " + curPri +
                                  " oldPri  " + oldPri);
    if(oldPri != LEVEL && oldPri != curPri) {
      remove(block, oldPri);
    }
    if(curPri != LEVEL && add(block, curPri)) {
      NameNode.stateChangeLog.debug(
                                    "BLOCK* NameSystem.UnderReplicationBlock.update:"
                                    + block
                                    + " has only "+curReplicas
                                    + " replicas and need " + curExpectedReplicas
                                    + " replicas so is added to neededReplications"
                                    + " at priority level " + curPri);
    }
    
  }
  
  @Override
  public int size() {
    // lockless operations
    int size = 0;
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_UNDER_HOME);
    sb.append(SLASH).append(PLACEHOLDER);
    int mark = sb.length()-1;
    Stat stat;
    synchronized(this) {
      for(int i = 0; i < LEVEL; i++) {
        sb.replace(mark, mark+1, String.valueOf(i));
        try {
          stat = ZKClient.getInstance().exist(sb.toString());
        } catch (IOException e) {
          LOG.error("IOException error when counting size for " 
              + sb.toString());
          continue;
        }
        if(stat != null) {
          size += stat.getNumChildren();
        }
      }
    }
    return size;
  }
  
  /** Return the priority of a block
   * @param block a under replication block
   * @param curReplicas current number of replicas of the block
   * @param expectedReplicas expected number of replicas of the block
   */
  @Override
  public int getPriority(Block block, int curReplicas,
      int decommissionedReplicas, int expectedReplicas) {
    if (curReplicas<0 || curReplicas>=expectedReplicas) {
      return LEVEL; // no need to replicate
    } else if(curReplicas==0) {
      // If there are zero non-decommissioned replica but there are
      // some decommissioned replicas, then assign them highest priority
      if (decommissionedReplicas > 0) {
        return 0;
      }
      return 2; // keep these blocks in needed replication.
    } else if(curReplicas==1) {
      return 0; // highest priority
    } else if(curReplicas*3<expectedReplicas) {
      return 1;
    } else {
      return 2;
    }
  }
  
  /**
   * Empty the queues.
   */
  @Override
  public void clear() {
    for(int i=0; i<LEVEL; i++) {
      mirror.get(i).clear();;
    }
  }
  
  /** return an iterator of all the under replication blocks */
  @Override
  public synchronized BlockIterator iterator() {
    return new BlockIterator();
  }
  
  /**
   * this list is for iterator only, every time the iterator is called, it will
   * clear old info and load new info from zookeeper.
   */
  private List<TreeSet<Block>> mirror = new ArrayList<TreeSet<Block>>();
  {
    for(int i=0; i<LEVEL; i++) {
      mirror.add(new TreeSet<Block>());
    }
  }
  
  static class ZkUnderBlockInfo {
    
    private static final String SEPARATOR = ";";
    private final long numBytes;
    private final long generationStamp;
    
    private ZkUnderBlockInfo(byte[] data) {
      String strData = new String(data, CHARSET);
      String[] datas = strData.split(SEPARATOR);
      numBytes = Long.parseLong(datas[0]);
      generationStamp = Long.parseLong(datas[1]);
    }
    
    private ZkUnderBlockInfo(Block block) {
      this.numBytes = block.getNumBytes();
      this.generationStamp = block.getGenerationStamp();
    }
    
    private byte[] toByteArray() {
      StringBuilder sb = new StringBuilder();
      sb.append(numBytes).append(SEPARATOR).append(generationStamp);
      return sb.toString().getBytes(CHARSET);
    }
    
  }
  
  class BlockIterator implements Iterator<Block> {
    private int level;
    private List<Iterator<Block>> iterators = new ArrayList<Iterator<Block>>();
    
    BlockIterator()
    {
      level=0;
      StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_UNDER_HOME);
      sb.append(SLASH).append(PLACEHOLDER);
      int mark = sb.length()-1;
      for(int i=0; i<LEVEL; i++) {
        mirror.get(i).clear();
        sb.replace(mark, mark+1, String.valueOf(i));
        load(sb.toString(), i);
        iterators.add(mirror.get(i).iterator());
      }
    }
    
    private void load(String levelPath, int level) {
      //lockless operation
      String childPath;
      byte[] data;
      try {
        List<String> children = ZKClient.getInstance().getChildren(levelPath, null);
        if(children == null) return;
        for(String child : children) {
          childPath = levelPath + SLASH + child;
          data = ZKClient.getInstance().getData(childPath, null);
          if(data != null) {
              ZkUnderBlockInfo blkInfo = new ZkUnderBlockInfo(data);
              mirror.get(level).add(new Block(Long.parseLong(child)
                  , blkInfo.numBytes, blkInfo.generationStamp));
          }
        }
        LOG.debug("BlockIterator load for " + levelPath);
      } catch (IOException e) {
        LOG.error("BlockIterator load failed");
      }
      
    }
            
    private void update() {
      while(level< LEVEL-1 && !iterators.get(level).hasNext()) {
        level++;
      }
    }
            
    public Block next() {
      update();
      return iterators.get(level).next();
    }
            
    public boolean hasNext() {
      update();
      return iterators.get(level).hasNext();
    }
            
    public void remove() {
      iterators.get(level).remove();
    }
    
    public int getPriority() {
      return level;
  };
}

}
