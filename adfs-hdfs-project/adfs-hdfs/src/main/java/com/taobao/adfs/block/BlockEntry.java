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

package com.taobao.adfs.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.adfs.datanode.Datanode;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class BlockEntry implements Comparable<BlockEntry> {
  private final long blockId;
  private final long length;
  private final long generationStamp;
  private final long fileId;
  private final int fileIndex;
  boolean onNullDatanode = false;
  final List<Block> blockList;
  final List<Block> blockListExcludeUnderConstruction;
  final org.apache.hadoop.hdfs.protocol.Block hdfsBlock;

  private BlockEntry(List<Block> blockList) {
    assert blockList != null : "blockList is null";
    assert !blockList.isEmpty() : "blockList is null";
    this.blockList = new ArrayList<Block>(blockList.size());
    blockListExcludeUnderConstruction = new ArrayList<Block>(blockList.size());
    Block primaryBlock = null;
    for (Block block : blockList) {
      if (primaryBlock == null || block.generationStamp > primaryBlock.generationStamp
          || (block.generationStamp == primaryBlock.generationStamp && block.version > primaryBlock.version))
        primaryBlock = block;
      if (block.datanodeId != Datanode.NULL_DATANODE_ID) {
        this.blockList.add(block);
        if (block.length >= 0) blockListExcludeUnderConstruction.add(block);
      } else onNullDatanode = true;
    }
    this.blockId = primaryBlock.id;
    this.length = primaryBlock.length;
    this.generationStamp = primaryBlock.generationStamp;
    this.fileId = primaryBlock.fileId;
    this.fileIndex = primaryBlock.fileIndex;
    hdfsBlock = new org.apache.hadoop.hdfs.protocol.Block(blockId, length, generationStamp);
  }

  public org.apache.hadoop.hdfs.protocol.Block getHdfsBlock() {
    return hdfsBlock;
  }

  public long getBlockId() {
    return blockId;
  }

  public List<Block> getBlockList(boolean excludeUnderConstruction) {
    return excludeUnderConstruction ? blockListExcludeUnderConstruction : blockList;
  }

  public Block getBlock(long datanodeId) {
    for (Block block : blockList) {
      if (block != null && block.datanodeId == datanodeId) return block;
    }
    return null;
  }

  public boolean onNullDatanode() {
    return onNullDatanode;
  }

  public long getFileId() {
    return fileId;
  }

  public long getGenerationStamp() {
    return generationStamp;
  }

  public int getFileIndex() {
    return fileIndex;
  }

  public long getLength() {
    return length;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BlockEntry [blockId=").append(blockId).append(", length=").append(length).append(
        ", generationStamp=").append(generationStamp).append(", fileId=").append(fileId).append(", fileIndex=").append(
        fileIndex).append(", datanodeIds={");
    for (int i = 0; i < blockList.size(); ++i) {
      builder.append(blockList.get(i).datanodeId);
      if (i < blockList.size() - 1) builder.append(',');
    }
    builder.append("}]");
    return builder.toString();
  }

  @Override
  public int compareTo(BlockEntry o) {
    return getFileIndex() - o.getFileIndex();
  }

  public static List<BlockEntry> getBlockEntryList(List<Block> blockList) {
    if (blockList == null) return null;
    Map<Long, List<Block>> blockIdToBlocks = new HashMap<Long, List<Block>>();
    for (Block block : blockList) {
      List<Block> blockListWithSameId = blockIdToBlocks.get(block.id);
      if (blockListWithSameId == null) blockIdToBlocks.put(block.id, blockListWithSameId = new ArrayList<Block>());
      blockListWithSameId.add(block);
    }

    List<BlockEntry> blockEntryList = new ArrayList<BlockEntry>();
    for (Long blockId : blockIdToBlocks.keySet()) {
      blockEntryList.add(new BlockEntry(blockIdToBlocks.get(blockId)));
    }
    return blockEntryList;
  }

  public static List<org.apache.hadoop.hdfs.protocol.Block> getHadoopBlock(List<BlockEntry> blockEntryList) {
    List<org.apache.hadoop.hdfs.protocol.Block> hadoopBlocks = new ArrayList<org.apache.hadoop.hdfs.protocol.Block>();
    if (blockEntryList == null) return hadoopBlocks;
    for (BlockEntry blockEntry : blockEntryList) {
      if (blockEntry == null) continue;
      org.apache.hadoop.hdfs.protocol.Block block =
          new org.apache.hadoop.hdfs.protocol.Block(blockEntry.getBlockId(), blockEntry.getLength(), blockEntry
              .getGenerationStamp());
      hadoopBlocks.add(block);
    }
    return hadoopBlocks;
  }

  public static org.apache.hadoop.hdfs.protocol.Block[] getHadoopBlockArray(List<BlockEntry> blockEntryList) {
    List<org.apache.hadoop.hdfs.protocol.Block> hadoopBlocks = getHadoopBlock(blockEntryList);
    return hadoopBlocks.toArray(new org.apache.hadoop.hdfs.protocol.Block[hadoopBlocks.size()]);
  }

  public static long getTotalLength(List<BlockEntry> blockEntryList) {
    long totalLength = 0;
    for (BlockEntry blockEntry : blockEntryList) {
      if (blockEntry.getLength() > 0) totalLength += blockEntry.getLength();
    }
    return totalLength;
  }

  public static BlockEntry getLastBlockEntry(List<BlockEntry> blockEntryList) {
    return (blockEntryList == null || blockEntryList.isEmpty()) ? null : blockEntryList.get(blockEntryList.size() - 1);
  }
}
