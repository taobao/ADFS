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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

public class DbNodeDirectory extends DbNode {
  
  public DbNodeDirectory(int fileId, FileStatus fs, 
      DbNodeManager manager) throws IOException {
    super(fileId, manager);
    dbLoad(fs);
  }
  
  protected void dbLoad(FileStatus fs) throws IOException {
    setModificationTime(fs.getModificationTime());
    setAccessTime(fs.getAccessTime());
    setFilePath(fs.getPath().toString());
  }
  
  public DbNodeDirectory(DbNodeManager manager) {
    super(manager);
  }

  @Override
  long[] computeContentSummary(long[] summary) {
    return null;
  }

  @Override
  public boolean isDirectory() {
    return true;
  }

}
