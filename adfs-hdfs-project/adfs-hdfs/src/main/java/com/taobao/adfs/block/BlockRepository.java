/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 \*   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.block;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.database.DatabaseExecutor.Comparator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class BlockRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(BlockRepository.class);

  public BlockRepository(Configuration conf) throws IOException {
    super(conf);
  }

  public List<Block> findAll() throws IOException {
    List<Block> blockList = find("PRIMARY", new Object[] { Long.MIN_VALUE }, Comparator.GE, Integer.MAX_VALUE);
    removeDeletedRows(blockList);
    return blockList;
  }

  public List<Block> findById(long id) throws IOException {
    List<Block> blocks = find("PRIMARY", new Object[] { id }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  public Block findByIdAndDatanodeId(long id, long datanodeId) throws IOException {
    Block block = (Block) findByKeys(new Object[] { id, datanodeId });
    if (block != null && block.version >= 0) return block;
    return null;
  }

  public List<Block> findByFileId(long fileId) throws IOException {
    List<Block> blocks = find("FILE_ID", new Object[] { fileId }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  public List<Block> findByDatanodeId(long datanodeId) throws IOException {
    List<Block> blocks = find("DATANODE_ID", new Object[] { datanodeId }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return Block.class;
  }

  @SuppressWarnings("unchecked")
  public List<Block> find(String indexName, Object[] keys, Comparator comparator, int limit) throws IOException {
    return (List<Block>) super.find(indexName, keys, comparator, limit);
  }

  public Block insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    return (Block) super.insert(row, overwrite);
  }

  public Block update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    return (Block) super.update(row, fieldsIndication);
  }

  public Block delete(DistributedDataRepositoryRow row) throws IOException {
    return (Block) super.delete(row);
  }

  public boolean isValid() {
    try {
      findInternal("PRIMARY", new Object[] { 0L, 0 }, Comparator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }
}
