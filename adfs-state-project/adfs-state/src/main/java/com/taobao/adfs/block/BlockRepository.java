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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.hs4j.FindOperator;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedException;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class BlockRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(BlockRepository.class);

  public BlockRepository(Configuration conf) throws IOException {
    super(conf);
  }

  public List<Block> findById(long id) throws IOException {
    List<Block> blocks = find("PRIMARY", new Object[] { id }, FindOperator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  public Block findByIdAndDatanodeId(long id, int datanodeId) throws IOException {
    Block block = (Block) findByKeys(new Object[] { id, datanodeId });
    if (block != null && block.version >= 0) return block;
    return null;
  }

  public List<Block> findByFileId(int fileId) throws IOException {
    List<Block> blocks = find("FILE_ID", new Object[] { fileId }, FindOperator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  public List<Block> findByDatanodeId(int datanodeId) throws IOException {
    List<Block> blocks = find("DATANODE_ID", new Object[] { datanodeId }, FindOperator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(blocks);
    return blocks;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return Block.class;
  }

  @SuppressWarnings("unchecked")
  public List<Block> find(String indexName, Object[] keys, FindOperator findOperator, int limit) throws IOException {
    return (List<Block>) super.find(indexName, keys, findOperator, limit);
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
      findInternal("PRIMARY", new Object[] { 0L, 0 }, FindOperator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  protected List<Block> findInternal(String indexName, Object[] keys, FindOperator findOperator, int limit)
      throws IOException {
    // get from database
    String[] stringKeys = new String[keys.length];
    for (int i = 0; i < keys.length; ++i) {
      stringKeys[i] = (keys[i] == null) ? null : keys[i].toString();
    }
    ResultSet resultSet = null;
    try {
      resultSet = databaseExecutor.findInternal(this, indexName, stringKeys, findOperator, limit, 0);
    } catch (Throwable t) {
      throw new DistributedException(true, "", t);
    }

    try {
      List<Block> blocks = new ArrayList<Block>();
      while (resultSet.next()) {
        // convert to block
        Block block = new Block();
        block.id = resultSet.getLong(1);
        block.datanodeId = resultSet.getInt(2);
        block.numbytes = resultSet.getLong(3);
        block.generationStamp = resultSet.getLong(4);
        block.fileId = resultSet.getInt(5);
        block.fileIndex = resultSet.getInt(6);
        block.version = resultSet.getLong(7);
        block.setOperateIdentifier(resultSet.getString(8));
        blocks.add(block);
      }
      resultSet.close();
      return blocks;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
