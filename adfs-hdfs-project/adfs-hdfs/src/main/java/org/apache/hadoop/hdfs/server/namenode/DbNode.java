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

 package org.apache.hadoop.hdfs.server.namenode;

import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

abstract class DbNode implements Comparable<DbNode> {
  
  protected int fileId;
  protected String filePath;
  protected long fileLen;
  protected long modificationTime;
  protected long accessTime;
  protected DbNodeManager dbFileManager;

  protected DbNode(DbNodeManager manager) {
    fileId = -1;
    dbFileManager = manager;
    filePath = null;
  }
  
  protected DbNode(int fid, DbNodeManager manager) {
    fileId = fid;
    dbFileManager = manager;
    filePath = null;
  }
  
  public int getFileId() {
    return fileId;
  }
  
  protected void setFileId(int fid) {
    fileId = fid;
  }
  
  public String getFilePath() {
    return filePath;
  }
  
  protected void setFilePath(String path) {
    filePath = path;
  }
  
  public long getLength() {
    return fileLen;
  }
  
  public void setLength(long len) {
    fileLen = len;
  }
  
  /** 
   * Get last modification time of inode.
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  protected void setModificationTime(long modtime) {
    assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  protected void setModificationTimeForce(long modtime) {
    assert !isDirectory();
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  protected void setAccessTime(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }
  
  /**
   * Check whether it's a directory
   */
  public abstract boolean isDirectory();
  

  /** Compute {@link ContentSummary}. */
  public final ContentSummary computeContentSummary() {
    long[] a = computeContentSummary(new long[]{0,0,0,0});
    return new ContentSummary(a[0], a[1], a[2], -1 /*getNsQuota()*/, 
                              a[3], -1 /*getDsQuota()*/);
  }
  /**
   * @return an array of three longs. 
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary);
  
  public int compareTo(DbNode dbNode) {
    return fileId - dbNode.fileId;
  }

  public boolean equals(Object o) {
    if (!(o instanceof DbNode)) {
      return false;
    }
    DbNode that = (DbNode)o;
    return fileId == that.fileId;
  }

  public int hashCode() {
    return fileId;
  }
  
  LocatedBlocks createLocatedBlocks(List<LocatedBlock> blocks) {
    return new LocatedBlocks(computeContentSummary().getLength(), blocks,
        isUnderConstruction());
  }
}
