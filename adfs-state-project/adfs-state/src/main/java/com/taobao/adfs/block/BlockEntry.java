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

import java.util.*;

/**
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class BlockEntry implements Comparable<BlockEntry> {

  public BlockEntry(Block block, List<Integer> datanodeIds) {
    this(block.id, block.numbytes, block.generationStamp, block.fileId, block.fileIndex, datanodeIds);
  }

  BlockEntry(long blockId, long numbytes, long generationStamp, int fileId, int index, List<Integer> datanodeIds) {
    this.blockId = blockId;
    this.numbytes = numbytes;
    this.generationStamp = generationStamp;
    this.fileId = fileId;
    this.fileIndex = index;
    this.datanodeIds = datanodeIds;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    BlockEntry other = (BlockEntry) obj;
    if (blockId != other.blockId) return false;
    if (datanodeIds == null) {
      if (other.datanodeIds != null) return false;
    } else if (!datanodeIds.equals(other.datanodeIds)) return false;
    if (fileId != other.fileId) return false;
    if (generationStamp != other.generationStamp) return false;
    if (fileIndex != other.fileIndex) return false;
    if (numbytes != other.numbytes) return false;
    return true;
  }

  public long getBlockId() {
    return blockId;
  }

  public List<Integer> getDatanodeIds() {
    return Collections.unmodifiableList(datanodeIds);
  }

  public int getFileId() {
    return fileId;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public int getIndex() {
    return fileIndex;
  }

  public long getNumbytes() {
    return numbytes;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (blockId ^ (blockId >>> 32));
    result = prime * result + ((datanodeIds == null) ? 0 : datanodeIds.hashCode());
    result = prime * result + (fileId ^ (fileId >>> 32));
    result = prime * result + (int) (generationStamp ^ (generationStamp >>> 32));
    result = prime * result + fileIndex;
    result = prime * result + (int) (numbytes ^ (numbytes >>> 32));
    return result;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BlockEntry [blockId=").append(blockId).append(", numbytes=").append(numbytes).append(
        ", generationStamp=").append(generationStamp).append(", fileId=").append(fileId).append(", index=").append(
        fileIndex).append(", datanodeIds=").append(datanodeIds).append("]");
    return builder.toString();
  }

  private final long blockId;
  private final long numbytes;
  private final long generationStamp;
  private final int fileId;
  private final int fileIndex;
  private final List<Integer> datanodeIds;

  @Override
  public int compareTo(BlockEntry o) {
    return getIndex() - o.getIndex();
  }

  public static List<BlockEntry> getBlockEntries(Block[] blocks) {
    Map<Long, List<Block>> blockIdToBlocks = new HashMap<Long, List<Block>>();
    for (Block block : blocks) {
      List<Block> blockList = blockIdToBlocks.get(block.id);
      if (blockList == null) blockIdToBlocks.put(block.id, blockList = new ArrayList<Block>());
      blockList.add(block);
    }

    List<BlockEntry> blockEntries = new ArrayList<BlockEntry>();
    for (Long blockId : blockIdToBlocks.keySet()) {
      List<Block> blockList = blockIdToBlocks.get(blockId);
      List<Integer> datanodeIdList = new ArrayList<Integer>();
      Block blockOfLastModified = null;
      for (Block block : blockList) {
        if (blockOfLastModified == null || blockOfLastModified.version < block.version) blockOfLastModified = block;
        if (block.datanodeId != BlockInternalProtocol.NONE_DATANODE_ID || blockList.size() == 1)
          datanodeIdList.add(block.datanodeId);
      }
      blockEntries.add(new BlockEntry(blockOfLastModified, datanodeIdList));
    }
    return blockEntries;
  }
}
