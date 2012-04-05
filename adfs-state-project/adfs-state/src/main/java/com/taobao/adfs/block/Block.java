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
  public Block() {
  }

  public Block(long id, long numbytes, long generationStamp, int fileId, int datanodeId, int fileIndex) {
    this(id, numbytes, generationStamp, fileId, datanodeId, fileIndex, -1L);
  }

  public Block(long id, long numbytes, long generationStamp, int fileId, int datanodeId, int fileIndex, long version) {
    this.id = id;
    this.numbytes = numbytes;
    this.generationStamp = generationStamp;
    this.fileId = fileId;
    this.datanodeId = datanodeId;
    this.fileIndex = fileIndex;
    this.version = version;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + datanodeId;
    result = prime * result + fileId;
    result = prime * result + fileIndex;
    result = prime * result + (int) (generationStamp ^ (generationStamp >>> 32));
    result = prime * result + (int) (id ^ (id >>> 32));
    result = prime * result + (int) (numbytes ^ (numbytes >>> 32));
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
    if (numbytes != other.numbytes) return false;
    if (version != other.version) return false;
    return true;
  }

  public Object[] getKeys() {
    return new Object[] { id, datanodeId };
  }

  public String[] getStringKeys() {
    return new String[] { String.valueOf(id), String.valueOf(datanodeId) };
  }

  public String[] getStringValues() {
    String[] fieldArray = new String[8];
    fieldArray[0] = String.valueOf(id);
    fieldArray[1] = String.valueOf(datanodeId);
    fieldArray[2] = String.valueOf(numbytes);
    fieldArray[3] = String.valueOf(generationStamp);
    fieldArray[4] = String.valueOf(fileId);
    fieldArray[5] = String.valueOf(fileIndex);
    fieldArray[6] = String.valueOf(version);
    fieldArray[7] = operateIdentifier;
    return fieldArray;
  }

  @Column(indexes = { @Index(name = "PRIMARY", index = 0) })
  public long id = 0;

  @Column(indexes = { @Index(name = "PRIMARY", index = 1), @Index(name = "DATANODE_ID") })
  public int datanodeId = 0;

  @Column()
  public long numbytes = 0;

  @Column()
  public long generationStamp = 0;

  @Column(indexes = { @Index(name = "FILE_ID") })
  public int fileId = 0;

  @Column()
  public int fileIndex = 0;

  @Column(indexes = { @Index(name = "VERSION", unique = true) })
  public long version = 0;

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long setVersion(long newVersion) {
    return version = newVersion;
  }

  @Override
  public Block clone() {
    Block block = new Block(id, numbytes, generationStamp, fileId, datanodeId, fileIndex, version);
    block.setOperateIdentifier(operateIdentifier);
    block.setNote(note);
    return block;
  }

  @Override
  public Block update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof Block)) return this;
    Block block = (Block) row;
    if ((fieldsIndication & 0x0001) == 0x0001) this.numbytes = block.numbytes;
    if ((fieldsIndication & 0x0002) == 0x0002) this.generationStamp = block.generationStamp;
    if ((fieldsIndication & 0x0004) == 0x0004) this.fileId = block.fileId;
    if ((fieldsIndication & 0x0008) == 0x0008) this.fileIndex = block.fileIndex;
    if ((fieldsIndication & Integer.MIN_VALUE) == Integer.MIN_VALUE) this.version = row.getVersion();
    return this;
  }
}
