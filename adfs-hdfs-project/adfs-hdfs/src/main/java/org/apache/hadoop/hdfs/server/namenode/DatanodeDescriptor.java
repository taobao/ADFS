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

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.state.StateManager;
/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
public class DatanodeDescriptor extends DatanodeInfo {
  /** Block and targets pair */
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeDescriptor[] targets;    

    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /** A BlockTargetPair queue. */
  private static class BlockQueue {
    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
      return blockq.offer(new BlockTargetPair(block, targets));
    }

    /** Dequeue */
    synchronized List<BlockTargetPair> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }
  }

  // isAlive == heartbeats.contains(this)
  // This is an optimization, because contains takes O(n) time on Arraylist
  public boolean isAlive = false;

  /** A queue of blocks to be replicated by this datanode */
  private BlockQueue replicateBlocks = new BlockQueue();
  /** A queue of blocks to be recovered by this datanode */
  private BlockQueue recoverBlocks = new BlockQueue();
  /** A set of blocks to be invalidated by this datanode */
  private Set<Block> invalidateBlocks = new TreeSet<Block>();

  /* Variables for maintaning number of blocks scheduled to be written to
   * this datanode. This count is approximate and might be slightly higger
   * in case of errors (e.g. datanode does not report if an error occurs 
   * while writing the block).
   */
  private int currApproxBlocksScheduled = 0;
  private int prevApproxBlocksScheduled = 0;
  private long lastBlocksScheduledRollTime = 0;
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min
  
  /** Default constructor */
  public DatanodeDescriptor() {}
  
  /** DatanodeDescriptor constructor
   * @param nodeID id of the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    this(nodeID, 0L, 0L, 0L, 0);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    this(nodeID, networkLocation, null);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param hostName it could be different from host specified for DatanodeID
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation,
                            String hostName) {
    this(nodeID, networkLocation, hostName, 0L, 0L, 0L, 0);
  }
  
  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param capacity capacity of the data node
   * @param dfsUsed space used by the data node
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
  }

  /** DatanodeDescriptor constructor
   * 
   * @param nodeID id of the data node
   * @param networkLocation location of the data node in network
   * @param capacity capacity of the data node, including space used by non-dfs
   * @param dfsUsed the used space by dfs datanode
   * @param remaining remaing capacity of the data node
   * @param xceiverCount # of data transfers at the data node
   */
  public DatanodeDescriptor(DatanodeID nodeID,
                            String networkLocation,
                            String hostName,
                            long capacity,
                            long dfsUsed,
                            long remaining,
                            int xceiverCount) {
    super(nodeID, networkLocation, hostName);
    updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
  }

  void resetBlocks() {
    this.capacity = 0;
    this.remaining = 0;
    this.dfsUsed = 0;
    this.xceiverCount = 0;
    this.invalidateBlocks.clear();
  }

  public int numBlocks() {
    // TODO using bm handler to query
    return 0; 
  }

  /**
   */
  void updateHeartbeat(long capacity, long dfsUsed, long remaining,
      int xceiverCount) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.lastUpdate = System.currentTimeMillis();
    this.xceiverCount = xceiverCount;
    rollBlocksScheduled(lastUpdate);
  }
  
  void updateFromBM(DatanodeInfo dnInfo) {
    this.name = dnInfo.getName(); // in case the datanode name is changed.
    this.capacity = dnInfo.getCapacity();
    this.dfsUsed = dnInfo.getDfsUsed();
    this.remaining = dnInfo.getRemaining();
    this.lastUpdate = dnInfo.getLastUpdate();
    this.xceiverCount = dnInfo.getXceiverCount();
    rollBlocksScheduled(lastUpdate);
  }
  
  /**
   * Iterates over the list of blocks belonging to the data-node.
   */
  static private class BlockIterator implements Iterator<Block> {

    @Override
    public boolean hasNext() {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public Block next() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void remove() {
      // TODO Auto-generated method stub
      
    }
  }

  /**
   * Store block replication work.
   */
  void addBlockToBeReplicated(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    replicateBlocks.offer(block, targets);
  }

  /**
   * Store block recovery work.
   */
  public void addBlockToBeRecovered(Block block, DatanodeDescriptor[] targets) {
    assert(block != null && targets != null && targets.length > 0);
    recoverBlocks.offer(block, targets);
  }

  /**
   * Store block invalidation work.
   */
  void addBlocksToBeInvalidated(List<Block> blocklist) {
    assert(blocklist != null && blocklist.size() > 0);
    synchronized (invalidateBlocks) {
      for(Block blk : blocklist) {
        invalidateBlocks.add(blk);
      }
    }
  }

  /**
   * The number of work items that are pending to be replicated
   */
  int getNumberOfBlocksToBeReplicated() {
    return replicateBlocks.size();
  }

  /**
   * The number of block invalidation items that are pending to 
   * be sent to the datanode
   */
  int getNumberOfBlocksToBeInvalidated() {
    synchronized (invalidateBlocks) {
      return invalidateBlocks.size();
    }
  }
  
  BlockCommand getReplicationCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = replicateBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blocktargetlist);
  }

  BlockCommand getLeaseRecoveryCommand(int maxTransfers) {
    List<BlockTargetPair> blocktargetlist = recoverBlocks.poll(maxTransfers);
    return blocktargetlist == null? null:
        new BlockCommand(DatanodeProtocol.DNA_RECOVERBLOCK, blocktargetlist);
  }
  
  /**
   * Remove the specified number of blocks to be invalidated
   */
  BlockCommand getInvalidateBlocks(int maxblocks) {
    Block[] deleteList = getBlockArray(invalidateBlocks, maxblocks);
    return deleteList == null ? null : new BlockCommand(
        DatanodeProtocol.DNA_INVALIDATE, deleteList);
  }

  static private Block[] getBlockArray(Collection<Block> blocks, int max) {
    Block[] blockarray = null;
    synchronized(blocks) {
      int available = blocks.size();
      int n = available;
      if (max > 0 && n > 0) {
        if (max < n) {
          n = max;
        }
        // allocate the properly sized block array ... 
        blockarray = new Block[n];

        // iterate tree collecting n blocks... 
        Iterator<Block> e = blocks.iterator();
        int blockCount = 0;

        while (blockCount < n && e.hasNext()) {
          // insert into array ... 
          blockarray[blockCount++] = e.next();

          // remove from tree via iterator, if we are removing 
          // less than total available blocks
          if (n < available){
            e.remove();
          }
        }
        assert(blockarray.length == n);
        
        // now if the number of blocks removed equals available blocks,
        // them remove all blocks in one fell swoop via clear
        if (n == available) { 
          blocks.clear();
        }
      }
    }
    return blockarray;
  }

  void reportDiff(StateManager stateManager,
                  DbNodeManager fileManager,
                  BlockListAsLongs newReport,
                  Collection<Block> toAdd,
                  Collection<Block> toRemove,
                  Collection<Block> toInvalidate) throws IOException {
    
    if(newReport == null)
      newReport = new BlockListAsLongs( new long[0]);
    
    /* we can get all blocks on this datanode using this method,
     * which is different from HDFS. Therefore, this difference has 
     * impact on the following logic*/
    Collection<BlockEntry> dnBlockIDList = 
      stateManager.getAllBlocksOnDatanode(this.getNodeid());
    
    // blkIdEntryMap is a helper structure to fast handle block
    Map<Long, BlockEntry> blkIdEntryMap =
      new HashMap<Long, BlockEntry>();
    Iterator<BlockEntry> iter = dnBlockIDList.iterator();
    while(iter.hasNext()) {
      BlockEntry be = iter.next();
      blkIdEntryMap.put(be.getBlockId(), be);
    }
    
    // scan the report and collect newly reported blocks
    // Note we are taking special precaution to limit tmp blocks allocated
    // as part this block report - which why block list is stored as longs
    Block iblk = new Block(); // a fixed new'ed block to be reused with index i
    for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
      iblk.set(newReport.getBlockId(i), newReport.getBlockLen(i), 
          newReport.getBlockGenStamp(i));
      // in memory fast handle for normal replica without RPC invoke
      BlockEntry storedBlock = blkIdEntryMap.get(iblk.getBlockId());
      if(storedBlock != null) {
        if(storedBlock.getGenerationStamp() == iblk.getGenerationStamp()) {
          /* here we can ensure it's a normal replica which
           * exists in both datanode physically and blockmap
           * , therefore we can safely remove it from dnBlockIDList*/
          blkIdEntryMap.remove(iblk.getBlockId());
          dnBlockIDList.remove(storedBlock);
          continue;
        }
      }
      // otherwise, we will have to invoke RPC methods
      try{
        if(stateManager.lock(60000, 30000, // use defaults
            1 /*for block lock*/, iblk.getBlockId())) {
          try {
            /* the block id is not found in dnBlockIDList or the block's genstamp
             * doesn't match with that in dnBlockIDList.*/
            checkAbnormalBlock(stateManager, dnBlockIDList, blkIdEntryMap,
                toAdd, toInvalidate, iblk);
          } finally {
            stateManager.unlock(1, iblk.getBlockId());
          }
        }
      } catch(IOException ignored) { }
    }
    
    // clean up helper structure
    blkIdEntryMap.clear();
    // collect blocks that have not been reported
    for(Iterator<BlockEntry> it = dnBlockIDList.iterator(); it.hasNext(); ) {
      BlockEntry be = it.next();
      int fileid = be.getFileId();
      DbNodeFile file = null;
      try{
        file = fileManager.getFile(fileid);
      } catch(IOException ignored) {}
      
      if(file == null || !file.isUnderConstruction()) {
	      Block blk = new Block(be.getBlockId(), be.getNumbytes(), be.getGenerationStamp());
	      toRemove.add(blk);
      }
    }
  }
  
  void checkAbnormalBlock(StateManager stateManager,
      Collection<BlockEntry> dnBlockIDList,
      Map<Long, BlockEntry> blkIdEntryMap,
      Collection<Block> toAdd,
      Collection<Block> toInvalidate,
      Block iblk) throws IOException {
    
    BlockEntry storedBlock = stateManager.getStoredBlockBy(iblk.getBlockId());
    if(storedBlock != null) {
      // if the block with a WILDCARD generation stamp matches 
      // then accept this block.
      // This block has a different generation stamp on the datanode 
      // because of a lease-recovery-attempt.
      if (storedBlock.getGenerationStamp() <= iblk.getGenerationStamp()) {
          // accept block. It wil be cleaned up on cluster restart.
      } else {
        storedBlock = null;
      }
    }
    
    if(storedBlock == null) {
      // If block is not in blocksMap it does not belong to any file
      toInvalidate.add(new Block(iblk));
      return;
    }
    
    boolean thisDNContainBlock = false;
    Collection<Integer> dnList = storedBlock.getDatanodeIds(); 
    if(dnList != null && dnList.contains(this.getNodeid())) {
      thisDNContainBlock = true;
    }
    
    if(!thisDNContainBlock) {// Known block, but not on the DN
      // if the size differs from what is in the blockmap, then return
      // the new block. addStoredBlock will then pick up the right size of this
      // block and will update the block object in the BlocksMap
      if (storedBlock.getNumbytes() != iblk.getNumBytes()) {
        toAdd.add(new Block(iblk));
      } else {
        toAdd.add(new Block(storedBlock.getBlockId(), storedBlock.getNumbytes(), storedBlock.getGenerationStamp()));
      }
      return;
    }
    
    /* this piece of code handles replica with newer genstamp,
     *  normal replica won't come here any more */
    BlockEntry beInList = blkIdEntryMap.get(storedBlock.getBlockId());
    if(beInList != null){
      blkIdEntryMap.remove(beInList.getBlockId());
      dnBlockIDList.remove(beInList);
    }
  }

  /** Serialization for FSEditLog */
  void readFieldsFromFSEditLog(DataInput in) throws IOException {
    this.name = UTF8.readString(in);
    this.storageID = UTF8.readString(in);
    this.infoPort = in.readShort() & 0x0000ffff;

    this.capacity = in.readLong();
    this.dfsUsed = in.readLong();
    this.remaining = in.readLong();
    this.lastUpdate = in.readLong();
    this.xceiverCount = in.readInt();
    this.location = Text.readString(in);
    this.hostName = Text.readString(in);
    setAdminState(WritableUtils.readEnum(in, AdminStates.class));
  }
  
  /**
   * @return Approximate number of blocks currently scheduled to be written 
   * to this datanode.
   */
  public int getBlocksScheduled() {
    return currApproxBlocksScheduled + prevApproxBlocksScheduled;
  }
  
  /**
   * Increments counter for number of blocks scheduled. 
   */
  void incBlocksScheduled() {
    currApproxBlocksScheduled++;
  }
  
  /**
   * Decrements counter for number of blocks scheduled.
   */
  void decBlocksScheduled() {
    if (prevApproxBlocksScheduled > 0) {
      prevApproxBlocksScheduled--;
    } else if (currApproxBlocksScheduled > 0) {
      currApproxBlocksScheduled--;
    } 
    // its ok if both counters are zero.
  }
  
  /**
   * Adjusts curr and prev number of blocks scheduled every few minutes.
   */
  private void rollBlocksScheduled(long now) {
    if ((now - lastBlocksScheduledRollTime) > 
        BLOCKS_SCHEDULED_ROLL_INTERVAL) {
      prevApproxBlocksScheduled = currApproxBlocksScheduled;
      currApproxBlocksScheduled = 0;
      lastBlocksScheduledRollTime = now;
    }
  }
}
