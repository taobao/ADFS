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

package com.taobao.adfs.datanode;

import java.io.*;

import com.taobao.adfs.distributed.DistributedData.DistributedRead;
import com.taobao.adfs.distributed.DistributedData.DistributedWrite;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public interface DatanodeInternalProtocol {
  public static final int NONE_DATANODE_ID = -1;

  @DistributedRead
  public Datanode[] findDatanodeById(int datanodeId) throws IOException;

  @DistributedRead
  public Datanode[] findDatanodeByStorageId(String storageId) throws IOException;

  @DistributedRead
  public Datanode[] findDatanodeByUpdateTime(long datanodeExpireTime) throws IOException;

  @DistributedWrite
  public Datanode[] updateDatanodeByDatanode(Datanode datanode) throws IOException;

  @DistributedWrite
  public Datanode[] insertDatanodeByDatanode(Datanode datanode) throws IOException;
}
