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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.Block;

public class DbNodeFile extends DbNode{
  
  protected short blockReplication;
  protected long preferredBlockSize;
  protected boolean blkLoaded; // for lazy load blocks
  protected BlockInfo blocks[] = null;
  
  public DbNodeFile(int fileId, 
      FileStatus fs, DbNodeManager manager) {
    super(fileId, manager);
    blkLoaded = false;
    dbLoad(fs);
  }
  
  public DbNodeFile(DbNodeManager manager) {
    super(manager);
    blkLoaded = false;
  }
  
  protected void dbLoad(FileStatus fs) {
    setFilePath(fs.getPath().toString());
    setLength(fs.getLen());
    setModificationTimeForce(fs.getModificationTime());
    setAccessTime(fs.getAccessTime());
    blockReplication = fs.getReplication();
    preferredBlockSize = fs.getBlockSize();    
  }
  
  protected boolean dbLoaded() {
    return (filePath != null);
  }
  
  public boolean isDirectory() {
    return false;
  }

  /**
   * Get block replication for the file 
   * @return block replication
   */
  public short getReplication() {
    return this.blockReplication;
  }

  protected void setReplication(short replication) {
    this.blockReplication = replication;
  }

  protected void lazyLoadBlocks() {
    if(!blkLoaded) {
      try {
        blocks = dbFileManager.getBlocks(this);
      } catch (IOException ignored) {};
      blkLoaded = true;
    }
  }
  
  public void refreshBlocks() throws IOException {
    blkLoaded = false;
  }
  
  /**
   * Get file blocks 
   * @return file blocks
   */
  public BlockInfo[] getBlocks() {
    lazyLoadBlocks();
    return this.blocks;
  }

  /**
   * Return the last block in this file, or null
   * if there are no blocks.
   */
  public BlockInfo getLastBlock() {
    lazyLoadBlocks();
    if (this.blocks == null ||
        this.blocks.length == 0)
      return null;
    return this.blocks[this.blocks.length - 1];
  }

  /**
   * add a block to the block list
   */
  public void addBlock(BlockInfo newblock) {
    lazyLoadBlocks();
    if (this.blocks == null) {
      this.blocks = new BlockInfo[1];
      this.blocks[0] = newblock;
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      this.blocks = newlist;
    }
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    lazyLoadBlocks();
    long bytes = 0;
    if (blocks != null) {
      for (Block blk : blocks) {
        bytes += blk.getNumBytes();
      }
    }
    summary[0] += bytes;
    summary[1]++;
    summary[3] += diskspaceConsumed(bytes);

    // update file length using calculated value
    fileLen = bytes;
    return summary;
  }

  long diskspaceConsumed(long size) {
    long diskspaceConsumed = 0;
    if(isUnderConstruction()) {
      /* If the last block is being written to, use prefferedBlockSize
       * rather than the actual block size. */
      if (blocks.length > 0 && blocks[blocks.length-1] != null) {
        size += preferredBlockSize - blocks[blocks.length-1].getNumBytes();
      }
      diskspaceConsumed = size * blockReplication;
    } else {
      /* we have some tricky method to calculate diskspaceConsumed
       * for a non-under-construction file */
      long numOfFullBlocks = size/preferredBlockSize;
      long lastBlockSize = size-numOfFullBlocks*preferredBlockSize;
      diskspaceConsumed = (numOfFullBlocks*preferredBlockSize + lastBlockSize)*blockReplication;
    }
    return diskspaceConsumed;
  }
  
  /**
   * Get the preferred block size of the file.
   * @return the number of bytes
   */
  public long getPreferredBlockSize() {
    return preferredBlockSize;
  }

  /**
   * Return the penultimate allocated block for this file.
   */
  public Block getPenultimateBlock() {
    lazyLoadBlocks();
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("[FILE:id=");
    sb.append(fileId)
      .append("|path=")
      .append(filePath)
      .append("]");
    return sb.toString();
  }

}
