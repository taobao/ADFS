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
  public static final long rootId = 0L;
  public static final File ROOT = new File(rootId, rootId, "");
  public static final int nameMaxLength = 255;
  @Column(indexes = { @Index(name = "PRIMARY") })
  public long id = rootId;
  @Column(nullable = false, indexes = { @Index(name = "PID_NAME", index = 0, unique = true) })
  public long parentId = rootId;
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
  @Column(width = 255, binary = true, indexes = { @Index(name = "LEASE_HOLDER") })
  public String leaseHolder = null;
  @Column()
  public long leaseRecoveryTime = 0;
  public String path = null;

  public File() {
  }

  public File(long id, long parentId, String name, long length, int blockSize, byte replication, long atime,
      long mtime, int owner, String leaseHolder, long lastRecoveryTime, long version, String path) {
    this.id = id;
    this.parentId = parentId;
    this.name = name;
    this.length = length;
    this.blockSize = blockSize;
    this.replication = replication;
    this.atime = atime;
    this.mtime = mtime;
    this.owner = owner;
    this.leaseHolder = leaseHolder;
    this.leaseRecoveryTime = lastRecoveryTime;
    this.version = version;
    this.path = path;
  }

  public File(long id, long parentId, String name, long length, int blockSize, byte replication, long atime,
      long mtime, int owner, long version) {
    this(id, parentId, name, length, blockSize, replication, atime, mtime, owner, null, 0, version, null);
  }

  public File(long id, long parentId, String name, long length, int blockSize, byte replication, long atime,
      long mtime, int owner) {
    this(id, parentId, name, length, blockSize, replication, atime, mtime, owner, 0);
  }

  public File(long parentId, String name, long length, int blockSize, byte replication, long atime, long mtime,
      int owner) {
    this(ROOT.id, parentId, name, length, blockSize, replication, atime, mtime, owner);
  }

  public File(long id, long parentId, String name, long length) {
    this(id, parentId, name, length, 0, (byte) 0, 0, 0, 0);
  }

  public File(long id, long parentId, String name) {
    this(id, parentId, name, -1);
  }

  public File(long parentId, String name) {
    this(ROOT.id, parentId, name);
  }

  static final public int PARENTID = 0x0001;
  static final public int NAME = PARENTID << 1;
  static final public int LENGTH = NAME << 1;
  static final public int BLOCKSIZE = LENGTH << 1;
  static final public int REPLICATION = BLOCKSIZE << 1;
  static final public int ATIME = REPLICATION << 1;
  static final public int MTIME = ATIME << 1;
  static final public int OWNER = MTIME << 1;
  static final public int LEASEHOLDER = OWNER << 1;
  static final public int LEASERECOVERYTIME = LEASEHOLDER << 1;

  @Override
  public File update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof File)) return this;
    File file = (File) row;
    if ((fieldsIndication & PARENTID) == PARENTID) this.parentId = file.parentId;
    if ((fieldsIndication & NAME) == NAME) this.name = file.name;
    if ((fieldsIndication & LENGTH) == LENGTH) this.length = file.length;
    if ((fieldsIndication & BLOCKSIZE) == BLOCKSIZE) this.blockSize = file.blockSize;
    if ((fieldsIndication & REPLICATION) == REPLICATION) this.replication = file.replication;
    if ((fieldsIndication & ATIME) == ATIME) this.atime = file.atime;
    if ((fieldsIndication & MTIME) == MTIME) this.mtime = file.mtime;
    if ((fieldsIndication & OWNER) == OWNER) this.owner = file.owner;
    if ((fieldsIndication & LEASEHOLDER) == LEASEHOLDER) this.leaseHolder = file.leaseHolder;
    if ((fieldsIndication & LEASERECOVERYTIME) == LEASERECOVERYTIME) this.leaseRecoveryTime = file.leaseRecoveryTime;
    if ((fieldsIndication & VERSION) == VERSION) this.version = row.getVersion();
    return this;
  }

  public boolean isRootByParentIdAndName() {
    return ROOT.id == parentId && ROOT.name.equals(name);
  }

  public boolean isRootById() {
    return ROOT.id == id;
  }

  public boolean isDir() {
    return length == -1;
  }

  public boolean isUnderConstruction() {
    return leaseHolder != null;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (atime ^ (atime >>> 32));
    result = prime * result + blockSize;
    result = prime * result + (int) (id ^ (id >>> 32));
    result = prime * result + ((leaseHolder == null) ? 0 : leaseHolder.hashCode());
    result = prime * result + (int) (leaseRecoveryTime ^ (leaseRecoveryTime >>> 32));
    result = prime * result + (int) (length ^ (length >>> 32));
    result = prime * result + (int) (mtime ^ (mtime >>> 32));
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + owner;
    result = prime * result + (int) (parentId ^ (parentId >>> 32));
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    result = prime * result + replication;
    result = prime * result + (int) (version ^ (version >>> 32));
    return result;
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
    if (leaseHolder == null) {
      if (other.leaseHolder != null) return false;
    } else if (!leaseHolder.equals(other.leaseHolder)) return false;
    if (leaseRecoveryTime != other.leaseRecoveryTime) return false;
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
}