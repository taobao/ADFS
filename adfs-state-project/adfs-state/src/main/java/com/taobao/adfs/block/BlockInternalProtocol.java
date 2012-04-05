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

import com.taobao.adfs.distributed.DistributedData.DistributedRead;
import com.taobao.adfs.distributed.DistributedData.DistributedWrite;
import com.taobao.adfs.file.File;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public interface BlockInternalProtocol {
  public static final int NONE_DATANODE_ID = -1;

  @DistributedRead
  public Block[] findBlockById(long id) throws IOException;

  @DistributedRead
  public Block[] findBlockByDatanodeId(int datanodeId) throws IOException;

  @DistributedRead
  public Block[] findBlockByFileId(int fileId) throws IOException;

  @DistributedRead
  public Block[] findBlockByFiles(File[] files) throws IOException;

  @DistributedWrite
  public Block[] insertBlockByBlock(Block block) throws IOException;

  @DistributedWrite
  public Block[] updateBlockByBlock(Block block) throws IOException;

  @DistributedWrite
  public Block[] deleteBlockById(long id) throws IOException;

  @DistributedWrite
  public Block[] deleteBlockByIdAndDatanodeId(long id, int datanodeId) throws IOException;

  @DistributedWrite
  public Block[] deleteBlockByFileId(int fileId) throws IOException;
}
