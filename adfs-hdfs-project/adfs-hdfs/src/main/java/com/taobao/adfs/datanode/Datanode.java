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

package com.taobao.adfs.datanode;

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
public class Datanode extends DistributedDataRepositoryRow {
  public static long NULL_DATANODE_ID = 0;
  @Column(indexes = { @Index(name = "PRIMARY") })
  public long id = 0;
  @Column(width = 32, indexes = { @Index(name = "NAME", unique = true) })
  public String name;
  @Column(width = 64, indexes = { @Index(name = "STORAGE_ID", unique = true) })
  public String storageId;
  @Column()
  public int ipcPort = 0;
  @Column()
  public int infoPort = 0;
  @Column()
  public long capacity = 0;
  @Column()
  public long dfsUsed = 0;
  @Column()
  public long remaining = 0;
  @Column(indexes = { @Index(name = "LAST_UPDATED") })
  public long lastUpdated = 0;
  @Column()
  public int xceiverCount = 0;
  @Column(width = 64)
  public String location;
  @Column(width = 32)
  public String adminState = null;

  public Datanode() {
  }

  public Datanode(long id, String name, String storageId, int ipcPort, int infoPort, long capacity, long dfsUsed,
      long remaining, long lastUpdated, int xceiverCount, String location, String adminState) {
    this(id, name, storageId, ipcPort, infoPort, capacity, dfsUsed, remaining, lastUpdated, xceiverCount, location,
        adminState, -1L);
  }

  public Datanode(long id, String name, String storageId, int ipcPort, int infoPort, long capacity, long dfsUsed,
      long remaining, long lastUpdated, int xceiverCount, String location, String adminState, long version) {
    this.id = id;
    this.name = name;
    this.storageId = storageId;
    this.ipcPort = ipcPort;
    this.infoPort = infoPort;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.lastUpdated = lastUpdated;
    this.xceiverCount = xceiverCount;
    this.location = location;
    this.adminState = adminState;
    this.version = version;
  }

  static final public int NAME = 0x0001;
  static final public int STORAGEID = NAME << 1;
  static final public int IPCPORT = STORAGEID << 1;
  static final public int INFOPORT = IPCPORT << 1;
  static final public int CAPACITY = INFOPORT << 1;
  static final public int DFSUSED = CAPACITY << 1;
  static final public int REMAINING = DFSUSED << 1;
  static final public int LASTUPDATED = REMAINING << 1;
  static final public int XCEIVERCOUNT = LASTUPDATED << 1;
  static final public int LOCATION = XCEIVERCOUNT << 1;
  static final public int ADMINSTATE = LOCATION << 1;

  @Override
  public Datanode update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof Datanode)) return this;
    Datanode datanode = (Datanode) row;
    if ((fieldsIndication & NAME) == NAME) this.name = datanode.name;
    if ((fieldsIndication & STORAGEID) == STORAGEID) this.storageId = datanode.storageId;
    if ((fieldsIndication & IPCPORT) == IPCPORT) this.ipcPort = datanode.ipcPort;
    if ((fieldsIndication & INFOPORT) == INFOPORT) this.infoPort = datanode.infoPort;
    if ((fieldsIndication & CAPACITY) == CAPACITY) this.capacity = datanode.capacity;
    if ((fieldsIndication & DFSUSED) == DFSUSED) this.dfsUsed = datanode.dfsUsed;
    if ((fieldsIndication & REMAINING) == REMAINING) this.remaining = datanode.remaining;
    if ((fieldsIndication & LASTUPDATED) == LASTUPDATED) this.lastUpdated = datanode.lastUpdated;
    if ((fieldsIndication & XCEIVERCOUNT) == XCEIVERCOUNT) this.xceiverCount = datanode.xceiverCount;
    if ((fieldsIndication & LOCATION) == LOCATION) this.location = datanode.location;
    if ((fieldsIndication & ADMINSTATE) == ADMINSTATE) this.adminState = datanode.adminState;
    if ((fieldsIndication & VERSION) == VERSION) this.version = row.getVersion();
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((adminState == null) ? 0 : adminState.hashCode());
    result = prime * result + (int) (capacity ^ (capacity >>> 32));
    result = prime * result + (int) (dfsUsed ^ (dfsUsed >>> 32));
    result = prime * result + (int) (id ^ (id >>> 32));
    result = prime * result + infoPort;
    result = prime * result + ipcPort;
    result = prime * result + (int) (lastUpdated ^ (lastUpdated >>> 32));
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + (int) (remaining ^ (remaining >>> 32));
    result = prime * result + ((storageId == null) ? 0 : storageId.hashCode());
    result = prime * result + (int) (version ^ (version >>> 32));
    result = prime * result + xceiverCount;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Datanode other = (Datanode) obj;
    if (adminState == null) {
      if (other.adminState != null) return false;
    } else if (!adminState.equals(other.adminState)) return false;
    if (capacity != other.capacity) return false;
    if (dfsUsed != other.dfsUsed) return false;
    if (id != other.id) return false;
    if (infoPort != other.infoPort) return false;
    if (ipcPort != other.ipcPort) return false;
    if (lastUpdated != other.lastUpdated) return false;
    if (location == null) {
      if (other.location != null) return false;
    } else if (!location.equals(other.location)) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (remaining != other.remaining) return false;
    if (storageId == null) {
      if (other.storageId != null) return false;
    } else if (!storageId.equals(other.storageId)) return false;
    if (version != other.version) return false;
    if (xceiverCount != other.xceiverCount) return false;
    return true;
  }

  public int getIp() {
    return (int) (id >>> 32);
  }

  public int getPort() {
    return (int) (id & 0xFFFFFFFF);
  }
}
