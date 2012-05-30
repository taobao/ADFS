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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.database.DatabaseExecutor.Comparator;
import com.taobao.adfs.distributed.DistributedDataBaseOnDatabase;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedLocker;
import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.util.HashQueue;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class FileRepository extends DistributedDataRepositoryBaseOnTable {
  public static final Logger logger = LoggerFactory.getLogger(FileRepository.class);
  HashQueue<Long> freeIds = null;
  int nameMaxLength = File.nameMaxLength;
  AtomicLong nextIdForFreeId = new AtomicLong(0);
  AtomicLong nextIdFailCount = new AtomicLong(0);
  Random random = new Random();

  public FileRepository(Configuration conf) throws IOException {
    super(conf);
    nameMaxLength = conf.getInt("file.name.length.max", File.nameMaxLength);
    if (nameMaxLength > File.nameMaxLength) {
      Utilities.logInfo(logger, "file.name.length.max is ", nameMaxLength, ", large than ", File.nameMaxLength,
          ", limit it to be ", File.nameMaxLength);
      nameMaxLength = File.nameMaxLength;
    }
    nextIdForFreeId.set(new Random().nextLong());
  }

  public File findById(long id) throws IOException {
    File file = (File) findByKeys(new Object[] { id });
    if (file != null && file.version >= 0) return file;
    return null;
  }

  public File findByParentIdAndName(long parentId, String name) throws IOException {
    List<File> files = find("PID_NAME", new Object[] { parentId, name }, Comparator.EQ, 1);
    if (!files.isEmpty() && files.get(0).version >= 0) return files.get(0);
    return null;
  }

  public List<File> findByParentId(long parentId) throws IOException {
    List<File> files = find("PID_NAME", new Object[] { parentId }, Comparator.EQ, Integer.MAX_VALUE);
    removeRootById(files);
    removeDeletedRows(files);
    return files;
  }

  public List<File> findByLeaseHolder(String leaseHolder) throws IOException {
    List<File> files = find("LEASE_HOLDER", new Object[] { leaseHolder }, Comparator.EQ, Integer.MAX_VALUE);
    removeDeletedRows(files);
    return files;
  }

  @Override
  public Class<? extends DistributedDataRepositoryRow> getRowClass() {
    return File.class;
  }

  @Override
  public FileCache getCache() throws IOException {
    if (cache != null) return (FileCache) cache;
    Utilities.logInfo(logger, "create file cache with capacity=", conf.getInt("file.cache.capacity", 0));
    return (FileCache) (cache = new FileCache(conf.getInt("file.cache.capacity", 0), getVersion()));
  }

  @Override
  synchronized protected void createMeta() throws IOException {
    File file = new File();
    file.atime = file.mtime = System.currentTimeMillis();
    if (findByKeys(File.ROOT.getKey()) == null) {
      file.name = File.ROOT.name;
      file = insert(file, true);
      Utilities.logInfo(logger, "create root=", file);
    }
  }

  @SuppressWarnings("unchecked")
  public List<File> find(String indexName, Object[] keys, Comparator comparator, int limit) throws IOException {
    if (indexName.equals("PID_NAME") && keys.length == 2) {
      String fileName = (String) keys[1];
      if (fileName == null) throw new IOException("fail to find file: name is null");
      if (fileName.length() > nameMaxLength)
        throw new IOException("fail to find by parentIdAndName=" + Utilities.deepToString(keys) + ": name length is "
            + fileName.length() + ", large than " + nameMaxLength);
    }
    return (List<File>) super.find(indexName, keys, comparator, limit);
  }

  public DistributedDataRepositoryRow findOldRowForInsert(DistributedDataRepositoryRow row) throws IOException {
    File file = (File) row;
    List<File> files = find("PID_NAME", new Object[] { file.parentId, file.name }, Comparator.EQ, 1);
    if (!files.isEmpty()) return files.get(0);
    else return null;
  }

  public File insert(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    return (File) super.insert(row, overwrite);
  }

  public File update(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    return (File) super.update(row, fieldsIndication);
  }

  public File delete(DistributedDataRepositoryRow row) throws IOException {
    return (File) super.delete(row);
  }

  public boolean isValid() {
    try {
      findInternal("PRIMARY", new Object[] { 0 }, Comparator.EQ, 1);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  protected File insertInternal(DistributedDataRepositoryRow row, boolean overwrite) throws IOException {
    if (row == null) throw new IOException("file is null");
    File file = (File) row;
    if (file.name == null) throw new IOException("fail to insert " + file + ": null name");
    if (file.name.isEmpty() && !file.isRootByParentIdAndName())
      throw new IOException("fail to insert " + file + ": empty name");
    if (file.name.length() > nameMaxLength)
      throw new IOException("fail to insert " + file + ": name length is " + file.name.length() + ", large than "
          + nameMaxLength);
    file.atime = file.mtime = System.currentTimeMillis();

    // check existence of this file for the efficacy
    List<File> oldFiles = find("PID_NAME", new Object[] { file.parentId, file.name }, Comparator.EQ, 1);
    File oldFile = oldFiles.isEmpty() ? null : oldFiles.get(0);
    if (oldFile != null && oldFile.version >= 0 && file.isDir() != oldFile.isDir())
      throw new IOException(oldFile + "->" + file + " :change type of oldFile=" + oldFile);
    if (oldFile != null && oldFile.version >= 0 && !overwrite && !file.isDir())
      throw new IOException("fail to insert " + file + ": overwrite=" + overwrite + ", oldFile=" + oldFile);
    if (oldFile != null && oldFile.version >= 0 && !overwrite && file.isDir()) return oldFile;

    // lock and check existence of this file again
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, file.parentId, file.name); // to avoid concurrent insert
    try {
      oldFiles = find("PID_NAME", new Object[] { file.parentId, file.name }, Comparator.EQ, 1);
      oldFile = oldFiles.isEmpty() ? null : oldFiles.get(0);
      if (oldFile != null && oldFile.version >= 0 && file.isDir() != oldFile.isDir())
        throw new IOException(oldFile + "->" + file + " :change type of oldFile=" + oldFile);
      if (oldFile != null && oldFile.version >= 0 && !overwrite && !file.isDir())
        throw new IOException("fail to insert " + file + ": overwrite=" + overwrite + ", oldFile=" + oldFile);
      if (oldFile != null && oldFile.version >= 0 && !overwrite && file.isDir()) {
        locker.unlock(null, file.parentId, file.name);
        return oldFile;
      }
    } catch (Throwable t) {
      locker.unlock(null, file.parentId, file.name);
      throw new IOException(t);
    }

    // lock existed file id or generate and lock new id
    try {
      file = (File) file.clone();
      if (oldFile != null) {
        file.id = oldFile.id;
        locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, file.id);
        if (find("PID_NAME", new Object[] { file.parentId, file.name }, Comparator.EQ, 1).isEmpty()) {
          locker.unlock(null, file.id);
          oldFile = null;
        }
      }
      if (oldFile == null) getUniqueIdAndLock(file);
    } catch (Throwable t) {
      locker.unlock(null, file.parentId, file.name);
      throw new IOException(t);
    }

    // lock parent file with readLock
    readWriteLocker.read(file.parentId);
    try {
      if (!file.isRootById()) {
        File parentFile = (File) findByKeys(new Object[] { file.parentId });
        if (parentFile == null || parentFile.version < 0)
          throw new IOException(file + ": parent not existed, parent=" + parentFile);
        if (!parentFile.isDir()) throw new IOException(file + ": parent is file, parent=" + parentFile);
      }
      // create or update this file
      file.version = getVersion().increaseAndGet();
      file.setIdentifier();
      if (oldFile == null) file = (File) insertPhysically(file);
      else file = (File) updatePhysically(oldFile, file);
      return file;
    } finally {
      readWriteLocker.unread(file.parentId);
      locker.unlock(null, file.id);
      locker.unlock(null, file.parentId, file.name);
    }
  }

  protected File updateInternal(DistributedDataRepositoryRow row, int fieldsIndication) throws IOException {
    if (row == null) throw new IOException("file is null");
    File file = (File) row;
    if ((fieldsIndication & 0x0002) != 0) {
      if (file.name == null) throw new IOException("fail to update " + file + ": null name");
      if (file.name.isEmpty() && !file.isRootById()) throw new IOException("fail to update " + file + ": empty name");
      if (file.name.length() > nameMaxLength)
        throw new IOException("fail to update " + file + ": name length is " + file.name.length() + ", large than "
            + nameMaxLength);
    }
    if ((fieldsIndication & 0x0001) != 0 && file.id == file.parentId && file.id != File.ROOT.id)
      throw new IOException("fail to update " + file + ": parent is self");

    // lock this file
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, file.id);
    File oldFile = null;
    try {
      oldFile = (File) findByKeys(new Object[] { file.id });
      if (oldFile == null || oldFile.version < 0) throw new IOException(file + ": not existed, oldFile=" + oldFile);
      if ((fieldsIndication & 0x0004) != 0 && oldFile.isDir() != file.isDir())
        throw new IOException(oldFile + "->" + file + ": change type, oldFile=" + oldFile);
      if ((fieldsIndication & 0x0001) != 0 && file.id == File.ROOT.id && oldFile.parentId != file.parentId)
        throw new IOException(oldFile + "->" + file + ": move root, oldFile=" + oldFile);
      if ((fieldsIndication & 0x0002) != 0 && file.id == File.ROOT.id && !file.name.equals(oldFile.name))
        throw new IOException(oldFile + "->" + file + ": rename root, oldFile=" + oldFile);
      file = (File) oldFile.clone().update(file, fieldsIndication);
    } catch (Throwable t) {
      locker.unlock(null, file.id);
      throw new IOException(t);
    }

    readWriteLocker.read(file.parentId);
    File newFile = null;
    try {
      if (!file.isRootById()) {
        // lock parent file with readLock
        File newParentFile = (File) findByKeys(new Object[] { file.parentId });
        if ((newParentFile == null || newParentFile.version < 0))
          throw new IOException(file + ": parent not existed, newParent=" + newParentFile);
        if (!newParentFile.isDir()) throw new IOException(file + ": parent is file, newParent=" + newParentFile);
      }
      // update this file
      file.version = getVersion().increaseAndGet();
      file.setIdentifier();
      newFile = (File) updatePhysically(oldFile, file);
    } finally {
      readWriteLocker.unread(file.parentId);
      locker.unlock(null, file.id);
    }

    // update modify time of parent file
    if (file.parentId != newFile.parentId) {
      File parentFile = new File();
      parentFile.mtime = newFile.mtime;
      parentFile.id = file.parentId;
      update(parentFile, File.MTIME);
      parentFile.id = newFile.parentId;
      update(parentFile, File.MTIME);
    }
    return newFile;
  }

  protected File deleteInternal(DistributedDataRepositoryRow row) throws IOException {
    if (row == null) throw new IOException("file is null");
    if (((File) row).id == File.ROOT.id) throw new IOException("fail to delete " + row + ": delete root");
    locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKey());
    readWriteLocker.write(row.getKey());
    try {
      File oldFile = (File) findByKeys(row.getKey());
      if (oldFile == null) return null;
      if (oldFile.isDir() && !findByParentId(oldFile.id).isEmpty())
        throw new IOException("fail to delete " + row + ": find child");
      File newFile = (File) oldFile.clone();
      newFile.version = -getVersion().increaseAndGet();
      return (File) deletePhysically(newFile);
    } finally {
      readWriteLocker.unwrite(row.getKey());
      locker.unlock(null, row.getKey());
    }
  }

  File getUniqueIdAndLock(File file) {
    if (file.isRootByParentIdAndName()) {
      file.id = File.ROOT.id;
      locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, file.id);
    } else {
      while (true) {
        if (conf.getBoolean("file.id.generate.random", true)) {
          long id = random.nextLong();
          if (id == 0) continue;
          if (locker.tryLock(null, Long.MAX_VALUE, id) == null) continue;
          try {
            if (findByKeys(new Object[] { id }, false) != null) {
              locker.unlock(null, id);
              continue;
            }
            // got a unique id
            file.id = id;
            Utilities.logTrace(logger, "getUniqueIdAndLockBySelf|newId=", id);
            return file;
          } catch (Throwable t) {
            locker.unlock(null, id);
            continue;
          }
        } else {
          if (freeIds.isEmpty()) {
            // generate a id
            long id = nextIdForFreeId.getAndAdd(nextIdFailCount.get() << 2);
            // root id is fixed, ignore it
            if (id == File.rootId) continue;;
            if (locker.tryLock(null, Long.MAX_VALUE, id) == null) continue;
            try {
              if (findByKeys(new Object[] { id }, false) != null) {
                nextIdFailCount.getAndDecrement();
                locker.unlock(null, id);
                continue;
              }
              if (freeIds.contains(id)) {
                nextIdFailCount.getAndDecrement();
                locker.unlock(null, id);
                continue;
              }
              nextIdFailCount.set(0);
              // got a unique id
              file.id = id;
              Utilities.logTrace(logger, "getUniqueIdAndLockBySelf|newId=", id);
              return file;
            } catch (Throwable t) {
              locker.unlock(null, id);
              continue;
            }
          } else {
            synchronized (freeIds) {
              Long freeId = freeIds.peek();
              if (freeId != null) {
                file.id = freeId;
                locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, file.id);
                freeIds.poll();
                break;
              }
            }
            // wait check thread to generate freeIds
            Utilities.sleepAndProcessInterruptedException(1, getLogger());
          }
        }
      }
    }
    return file;
  }

  synchronized public void open(DistributedDataBaseOnDatabase distributedData) throws IOException {
    freeIds = new HashQueue<Long>();
    super.open(distributedData);
  }

  synchronized public void close() throws IOException {
    super.close();
    freeIds = null;
  }

  void prepareFreeIds() {
    int capacity = conf.getInt("file.id.prepare.number", 0);
    if (capacity <= 0) {
      Utilities.sleepAndProcessInterruptedException(10, getLogger());
      return;
    }

    if (!ServerType.MASTER.toString().equals(conf.get("distributed.server.type"))) {
      freeIds.clear();
      DistributedMetrics.intValueaSet("dataRepository.prepareFileIdNumber", freeIds.size());
      DistributedMetrics.intValueaSet("dataRepository.prepareFileIdCapacity", capacity);
      Utilities.logTrace(logger, "freeIds|size=", freeIds.size(), " serverType=", conf.get("distributed.server.type"));
      Utilities.sleepAndProcessInterruptedException(1000, getLogger());
      return;
    }

    HashQueue<Long> freeIds = this.freeIds;
    DistributedLocker locker = this.locker;
    if (freeIds == null || locker == null) return;
    Utilities.sleepAndProcessInterruptedException(10, getLogger());
    if (freeIds.size() >= capacity) return;

    // generate a random id
    long id = nextIdForFreeId.getAndAdd(nextIdFailCount.get() << 2);
    // root id is fixed, ignore it
    if (id == File.rootId) return;
    // consumer has locked this id and ready to poll it
    if (locker.tryLock(null, Long.MAX_VALUE, id) == null) return;
    try {
      // duplicated id
      if (freeIds.contains(id)) {
        nextIdFailCount.getAndDecrement();
        return;
      }
      if (findByKeys(new Object[] { id }, false) != null) {
        nextIdFailCount.getAndDecrement();
        return;
      }
      nextIdFailCount.set(0);
      // got a unique id and add it to freeIds
      freeIds.add(id);
      DistributedMetrics.intValueaSet("dataRepository.prepareFileIdNumber", freeIds.size());
      DistributedMetrics.intValueaSet("dataRepository.prepareFileIdCapacity", capacity);
      Utilities.logTrace(logger, "freeIds|size=", freeIds.size(), "|newId=", id);
    } catch (Throwable t) {
      Utilities.logWarn(getLogger(), t);
    } finally {
      locker.unlock(null, id);
    }
  }

  protected void checkThreadTasks() {
    prepareFreeIds();
  }

  List<File> removeRootById(List<File> files) {
    if (files == null) return null;
    for (int i = 0; i < files.size(); ++i) {
      if (files.get(i).id == File.ROOT.id) {
        files.remove(i);
        break;
      }
    }
    return files;
  }
}
