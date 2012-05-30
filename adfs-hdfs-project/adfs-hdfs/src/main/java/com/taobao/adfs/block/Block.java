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

import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Column;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Database;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.DistributedDataRepositoryRow;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Index;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Table;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
@Database(name = "nn_state")
@Table()
public class Block extends DistributedDataRepositoryRow {
  @Column(indexes = { @Index(name = "PRIMARY", index = 0) })
  public long id = 0;
  @Column(indexes = { @Index(name = "PRIMARY", index = 1), @Index(name = "DATANODE_ID") })
  public long datanodeId = 0;
  @Column()
  public long length = -1;
  @Column()
  public long generationStamp = 0;
  @Column(indexes = { @Index(name = "FILE_ID") })
  public long fileId = 0;
  @Column()
  public int fileIndex = 0;

  public Block() {
  }

  public Block(long id, long length, long generationStamp, long fileId, long datanodeId, int fileIndex) {
    this(id, length, generationStamp, fileId, datanodeId, fileIndex, -1L);
  }

  public Block(long id, long length, long generationStamp, long fileId, long datanodeId, int fileIndex, long version) {
    this.id = id;
    this.length = length;
    this.generationStamp = generationStamp;
    this.fileId = fileId;
    this.datanodeId = datanodeId;
    this.fileIndex = fileIndex;
    this.version = version;
  }

  static final public int LENGTH = 0x0001;
  static final public int GENERATIONSTAMP = LENGTH << 1;
  static final public int FILEID = GENERATIONSTAMP << 1;
  static final public int FILEINDEX = FILEID << 1;

  @Override
  public Block update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof Block)) return this;
    Block block = (Block) row;
    if ((fieldsIndication & LENGTH) == LENGTH) this.length = block.length;
    if ((fieldsIndication & GENERATIONSTAMP) == GENERATIONSTAMP) this.generationStamp = block.generationStamp;
    if ((fieldsIndication & FILEID) == FILEID) this.fileId = block.fileId;
    if ((fieldsIndication & FILEINDEX) == FILEINDEX) this.fileIndex = block.fileIndex;
    if ((fieldsIndication & VERSION) == VERSION) this.version = row.getVersion();
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (datanodeId ^ (datanodeId >>> 32));
    result = prime * result + (int) (fileId ^ (fileId >>> 32));
    result = prime * result + fileIndex;
    result = prime * result + (int) (generationStamp ^ (generationStamp >>> 32));
    result = prime * result + (int) (id ^ (id >>> 32));
    result = prime * result + (int) (length ^ (length >>> 32));
    result = prime * result + (int) (version ^ (version >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Block other = (Block) obj;
    if (datanodeId != other.datanodeId) return false;
    if (fileId != other.fileId) return false;
    if (fileIndex != other.fileIndex) return false;
    if (generationStamp != other.generationStamp) return false;
    if (id != other.id) return false;
    if (length != other.length) return false;
    if (version != other.version) return false;
    return true;
  }
}
