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

package com.taobao.adfs.block;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.taobao.adfs.file.File;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public interface BlockProtocol {
  static public final int lockId = 1;

  /**
   * Add a new block to file, but not uploaded. .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public void addBlockToFileBy(org.apache.hadoop.hdfs.protocol.Block block, int fileId, int fileIndex)
      throws IOException;

  public void addBlockToFile(org.apache.hadoop.hdfs.protocol.Block block, int fileId, int fileIndex,
      boolean removeBeforeAdding) throws IOException;

  /**
   * Get all blocks on a special datanode .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public Collection<BlockEntry> getAllBlocksOnDatanode(int datanodeId) throws IOException;

  /**
   * Get a List of BlockEntry of a special file .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public List<BlockEntry> getBlocksByFileID(int fileId) throws IOException;

  public List<BlockEntry> getBlocksByFiles(File[] files) throws IOException;

  public BlockEntry getStoredBlockBy(long blockId) throws IOException;

  public BlockEntry getStoredBlockBy(long blockId, long generationStamp) throws IOException;

  public boolean isBlockIdExists(long blockId) throws IOException;

  /**
   * Add a blockid->dnid record, means that a block received command from a datanode.
   * 
   * @param numbytes
   * @param generationStamp
   * @throws IOException
   *           when some connect problem occurs
   */
  public void receiveBlockFromDatanodeBy(int datanodeId, long blockId, long numbytes, long generationStamp)
      throws IOException;

  /**
   * Add a blockid->dnid record and update all numbytes for all block with same block id
   */
  public void receiveBlockUpdateBytesFromDatanodeBy(int datanodeId, long blockId, long numbytes, long generationStamp)
      throws IOException;

  /**
   * Delete a block from blocksmap, include the block record and it's block->dnid map record .
   * 
   * @throws IOException
   *           when some connect problem occurs
   * 
   * @param blockId
   *          id of a block.
   */
  public void removeBlock(long blockId) throws IOException;

  public void removeBlockReplicationOnDatanodeBy(int datanodeId, long blockId) throws IOException;

  /**
   * When delete a file, delelte all it's blocks and blocks' dn map records .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public void removeBlocksOfFile(int fileId) throws IOException;

  /**
   * Get the datanode id list which the block belong to.
   * 
   * @throws IOException
   *           when some connect problem occurs
   * @deprecated please use {@link BlockEntry#getDatanodeIds()} instead.
   */
  @Deprecated
  public Collection<Integer> getDatanodeList(long blockid) throws IOException;
}
