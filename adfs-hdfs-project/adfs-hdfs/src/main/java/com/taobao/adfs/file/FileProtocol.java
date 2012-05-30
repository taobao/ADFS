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

import java.io.IOException;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public interface FileProtocol {
  static public final int lockId = 0;

  public boolean complete(int id, long length) throws IOException;

  public File create(String path, boolean overwrite, byte replication, int blockSize) throws IOException;

  public File[] delete(String path) throws IOException;

  public File[] delete(String path, boolean recursive) throws IOException;

  public File[] delete(File file, boolean recursive) throws IOException;

  public File[] delete(File[] files, boolean recursive) throws IOException;

  public File getFileInfo(int id) throws IOException;

  public File getFileInfo(String path) throws IOException;

  public File[] getListing(String path) throws IOException;

  public File[] getDescendant(String path, boolean exculudeDir, boolean includeSelfAnyway) throws IOException;

  public File[] getDescendant(File file, boolean exculudeDir, boolean includeSelfAnyway) throws IOException;

  public boolean mkdirs(String path) throws IOException;

  public File[] rename(String path, String targetPath) throws IOException;

  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException;

  public short setReplication(String path, byte replication) throws IOException;

  public void setTimes(String path, long mtime, long atime) throws IOException;
}
