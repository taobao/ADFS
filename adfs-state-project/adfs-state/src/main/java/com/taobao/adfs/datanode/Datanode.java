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
  public Datanode() {
  }

  public Datanode(int id, String name, String storageId, int ipcPort, int infoPort, int layoutVersion, int namespaceId,
      long ctime, long capacity, long dfsUsed, long remaining, long lastUpdated, int xceiverCount, String location,
      String hostName, byte adminState) {
    this(id, name, storageId, ipcPort, infoPort, layoutVersion, namespaceId, ctime, capacity, dfsUsed, remaining,
        lastUpdated, xceiverCount, location, hostName, adminState, -1L);
  }

  public Datanode(int id, String name, String storageId, int ipcPort, int infoPort, int layoutVersion, int namespaceId,
      long ctime, long capacity, long dfsUsed, long remaining, long lastUpdated, int xceiverCount, String location,
      String hostName, byte adminState, long version) {
    this.id = id;
    this.name = name;
    this.storageId = storageId;
    this.ipcPort = ipcPort;
    this.infoPort = infoPort;
    this.layoutVersion = layoutVersion;
    this.namespaceId = namespaceId;
    this.ctime = ctime;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.lastUpdated = lastUpdated;
    this.xceiverCount = xceiverCount;
    this.location = location;
    this.hostName = hostName;
    this.adminState = adminState;
    this.version = version;
  }

  @Column(indexes = { @Index(name = "PRIMARY") })
  public int id = 0;

  @Column(width = 32)
  public String name;

  @Column(width = 64, indexes = { @Index(name = "STORAGE_ID", unique = true) })
  public String storageId;

  @Column()
  public int ipcPort = 0;

  @Column()
  public int infoPort = 0;

  @Column()
  public int layoutVersion = 0;

  @Column()
  public int namespaceId = 0;

  @Column()
  public long ctime = 0;

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
  public String hostName;

  @Column()
  public byte adminState = 0;

  @Column(indexes = { @Index(name = "VERSION", unique = true) })
  public long version = 0;

  public Object[] getKeys() {
    return new Object[] { id };
  }

  public String[] getStringKeys() {
    return new String[] { String.valueOf(id) };
  }

  public String[] getStringValues() {
    String[] fieldArray = new String[18];
    fieldArray[0] = String.valueOf(id);
    fieldArray[1] = name;
    fieldArray[2] = storageId;
    fieldArray[3] = String.valueOf(ipcPort);
    fieldArray[4] = String.valueOf(infoPort);
    fieldArray[5] = String.valueOf(layoutVersion);
    fieldArray[6] = String.valueOf(namespaceId);
    fieldArray[7] = String.valueOf(ctime);
    fieldArray[8] = String.valueOf(capacity);
    fieldArray[9] = String.valueOf(dfsUsed);
    fieldArray[10] = String.valueOf(remaining);
    fieldArray[11] = String.valueOf(lastUpdated);
    fieldArray[12] = String.valueOf(xceiverCount);
    fieldArray[13] = location;
    fieldArray[14] = hostName;
    fieldArray[15] = String.valueOf(adminState);
    fieldArray[16] = String.valueOf(version);
    fieldArray[17] = operateIdentifier;
    return fieldArray;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + adminState;
    result = prime * result + (int) (capacity ^ (capacity >>> 32));
    result = prime * result + (int) (ctime ^ (ctime >>> 32));
    result = prime * result + (int) (dfsUsed ^ (dfsUsed >>> 32));
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + id;
    result = prime * result + infoPort;
    result = prime * result + ipcPort;
    result = prime * result + (int) (lastUpdated ^ (lastUpdated >>> 32));
    result = prime * result + layoutVersion;
    result = prime * result + ((location == null) ? 0 : location.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + namespaceId;
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
    if (adminState != other.adminState) return false;
    if (capacity != other.capacity) return false;
    if (ctime != other.ctime) return false;
    if (dfsUsed != other.dfsUsed) return false;
    if (hostName == null) {
      if (other.hostName != null) return false;
    } else if (!hostName.equals(other.hostName)) return false;
    if (id != other.id) return false;
    if (infoPort != other.infoPort) return false;
    if (ipcPort != other.ipcPort) return false;
    if (lastUpdated != other.lastUpdated) return false;
    if (layoutVersion != other.layoutVersion) return false;
    if (location == null) {
      if (other.location != null) return false;
    } else if (!location.equals(other.location)) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (namespaceId != other.namespaceId) return false;
    if (remaining != other.remaining) return false;
    if (storageId == null) {
      if (other.storageId != null) return false;
    } else if (!storageId.equals(other.storageId)) return false;
    if (version != other.version) return false;
    if (xceiverCount != other.xceiverCount) return false;
    return true;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long setVersion(long newVersion) {
    return version = newVersion;
  }

  @Override
  public Datanode clone() {
    Datanode datanode =
        new Datanode(id, name, storageId, ipcPort, infoPort, layoutVersion, namespaceId, ctime, capacity, dfsUsed,
            remaining, lastUpdated, xceiverCount, location, hostName, adminState, version);
    datanode.setOperateIdentifier(operateIdentifier);
    datanode.setNote(note);
    return datanode;
  }

  @Override
  public Datanode update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof Datanode)) return this;
    Datanode datanode = (Datanode) row;
    if ((fieldsIndication & 0x0001) == 0x0001) this.name = datanode.name;
    if ((fieldsIndication & 0x0002) == 0x0002) this.storageId = datanode.storageId;
    if ((fieldsIndication & 0x0004) == 0x0004) this.ipcPort = datanode.ipcPort;
    if ((fieldsIndication & 0x0008) == 0x0008) this.infoPort = datanode.infoPort;
    if ((fieldsIndication & 0x0010) == 0x0010) this.layoutVersion = datanode.layoutVersion;
    if ((fieldsIndication & 0x0020) == 0x0020) this.namespaceId = datanode.namespaceId;
    if ((fieldsIndication & 0x0040) == 0x0040) this.ctime = datanode.ctime;
    if ((fieldsIndication & 0x0080) == 0x0080) this.capacity = datanode.capacity;
    if ((fieldsIndication & 0x0100) == 0x0100) this.dfsUsed = datanode.dfsUsed;
    if ((fieldsIndication & 0x0200) == 0x0200) this.remaining = datanode.remaining;
    if ((fieldsIndication & 0x0400) == 0x0400) this.lastUpdated = datanode.lastUpdated;
    if ((fieldsIndication & 0x0800) == 0x0800) this.xceiverCount = datanode.xceiverCount;
    if ((fieldsIndication & 0x1000) == 0x1000) this.location = datanode.location;
    if ((fieldsIndication & 0x2000) == 0x2000) this.hostName = datanode.hostName;
    if ((fieldsIndication & 0x4000) == 0x4000) this.adminState = datanode.adminState;
    if ((fieldsIndication & Integer.MIN_VALUE) == Integer.MIN_VALUE) this.version = row.getVersion();
    return this;
  }
}
