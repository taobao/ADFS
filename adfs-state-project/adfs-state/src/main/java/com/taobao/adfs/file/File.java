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

package com.taobao.adfs.file;

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
public class File extends DistributedDataRepositoryRow {
  public static final int rootId = 0;
  public static final File ROOT = new File(rootId, rootId, "");
  public static final int nameMaxLength = 255;

  @Column(indexes = { @Index(name = "PRIMARY") })
  public int id = rootId;

  @Column(nullable = false, indexes = { @Index(name = "PID_NAME", index = 0, unique = true) })
  public int parentId = rootId;

  @Column(width = 255, binary = true, indexes = { @Index(name = "PID_NAME", index = 1, unique = true) })
  public String name = null;

  @Column()
  public long length = -1;

  @Column()
  public int blockSize = 0;

  @Column()
  public byte replication = 0;

  @Column()
  public long atime = 0;

  @Column()
  public long mtime = 0;

  @Column()
  public int owner = 0;

  @Column(indexes = { @Index(name = "VERSION", unique = true) })
  public long version = 0;

  public String path = null;

  public File() {
  }

  public File(int id, int parentId, String name, long length, int blockSize, byte replication, long atime, long mtime,
      int owner, long version, String path) {
    this.id = id;
    this.parentId = parentId;
    this.name = name;
    this.length = length;
    this.blockSize = blockSize;
    this.replication = replication;
    this.atime = atime;
    this.mtime = mtime;
    this.owner = owner;
    this.version = version;
    this.path = path;
  }

  public File(int id, int parentId, String name, long length, int blockSize, byte replication, long atime, long mtime,
      int owner, long version) {
    this(id, parentId, name, length, blockSize, replication, atime, mtime, owner, version, null);
  }

  public File(int id, int parentId, String name, long length, int blockSize, byte replication, long atime, long mtime,
      int owner) {
    this(id, parentId, name, length, blockSize, replication, atime, mtime, owner, 0);
  }

  public File(int parentId, String name, long length, int blockSize, byte replication, long atime, long mtime, int owner) {
    this(ROOT.id, parentId, name, length, blockSize, replication, atime, mtime, owner);
  }

  public File(int id, int parentId, String name, long length) {
    this(id, parentId, name, length, 0, (byte) 0, 0, 0, 0);
  }

  public File(int id, int parentId, String name) {
    this(id, parentId, name, -1);
  }

  public File(int parentId, String name) {
    this(ROOT.id, parentId, name);
  }

  public boolean isDir() {
    return length == -1;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    File other = (File) obj;
    if (atime != other.atime) return false;
    if (blockSize != other.blockSize) return false;
    if (id != other.id) return false;
    if (length != other.length) return false;
    if (mtime != other.mtime) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (owner != other.owner) return false;
    if (parentId != other.parentId) return false;
    if (path == null) {
      if (other.path != null) return false;
    } else if (!path.equals(other.path)) return false;
    if (replication != other.replication) return false;
    if (version != other.version) return false;
    return true;
  }

  public int hashCode() {
    return Integer.valueOf(id).hashCode();
  }

  @Override
  public File clone() {
    File file = new File(id, parentId, name, length, blockSize, replication, atime, mtime, owner, version, path);
    file.setOperateIdentifier(operateIdentifier);
    file.setNote(note);
    return file;
  }

  @Override
  public File update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof File)) return this;
    File file = (File) row;
    if ((fieldsIndication & 0x0001) == 0x0001) this.parentId = file.parentId;
    if ((fieldsIndication & 0x0002) == 0x0002) this.name = file.name;
    if ((fieldsIndication & 0x0004) == 0x0004) this.length = file.length;
    if ((fieldsIndication & 0x0008) == 0x0008) this.blockSize = file.blockSize;
    if ((fieldsIndication & 0x0010) == 0x0010) this.replication = file.replication;
    if ((fieldsIndication & 0x0020) == 0x0020) this.atime = file.atime;
    if ((fieldsIndication & 0x0040) == 0x0040) this.mtime = file.mtime;
    if ((fieldsIndication & 0x0080) == 0x0080) this.owner = file.owner;
    if ((fieldsIndication & Integer.MIN_VALUE) == Integer.MIN_VALUE) this.version = row.getVersion();
    return this;
  }

  @Override
  public Object[] getKeys() {
    return new Object[] { id };
  }

  @Override
  public String[] getStringKeys() {
    return new String[] { String.valueOf(id) };
  }

  @Override
  public String[] getStringValues() {
    String[] fieldArray = new String[11];
    fieldArray[0] = Integer.toString(id);
    fieldArray[1] = Integer.toString(parentId);
    fieldArray[2] = name;
    fieldArray[3] = Long.toString(length);
    fieldArray[4] = Integer.toString(blockSize);
    fieldArray[5] = Byte.toString(replication);
    fieldArray[6] = Long.toString(atime);
    fieldArray[7] = Long.toString(mtime);
    fieldArray[8] = Integer.toString(owner);
    fieldArray[9] = Long.toString(version);
    fieldArray[10] = operateIdentifier;
    return fieldArray;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long setVersion(long newVersion) {
    return version = newVersion;
  }

  public boolean isRootByParentIdAndName() {
    return ROOT.id == parentId && ROOT.name.equals(name);
  }

  public boolean isRootById() {
    return ROOT.id == id;
  }
}