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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.adfs.distributed.DistributedDataCache;
import com.taobao.adfs.distributed.DistributedDataVersion;
import com.taobao.adfs.util.Cloneable;
import com.taobao.adfs.util.DeepArray;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class FileCache extends DistributedDataCache {
  AtomicLong getNumberOfId = new AtomicLong(0);
  AtomicLong hitNumberOfId = new AtomicLong(0);
  AtomicLong getNumberOfParentIdAndName = new AtomicLong(0);
  AtomicLong hitNumberOfParentIdAndName = new AtomicLong(0);
  Map<DeepArray, CacheValue> cacheOfId = new ConcurrentHashMap<DeepArray, CacheValue>();
  Map<DeepArray, CacheValue> cacheOfParentIdAndName = new ConcurrentHashMap<DeepArray, CacheValue>();

  public FileCache(int capacity, DistributedDataVersion version) {
    super(capacity);
  }

  @Override
  protected void addForInsertInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      addInternal("PRIMARY", new Object[] { file.id }, file);
      addInternal("PID_NAME", new Object[] { file.parentId, file.name }, file);
    }
  }

  @Override
  protected void addForUpdateInternal(Cloneable oldValue, Cloneable newValue) {
    if (oldValue instanceof File && newValue instanceof File) {
      File oldFile = (File) oldValue;
      File newFile = (File) newValue;
      addInternal("PRIMARY", new Object[] { newFile.id }, newFile);
      addInternal("PID_NAME", new Object[] { newFile.parentId, newFile.name }, newFile);
      if (newFile.parentId != oldFile.parentId || !newFile.name.equals(oldFile.name))
        addInternal("PID_NAME", new Object[] { oldFile.parentId, oldFile.name }, null);
    }
  }

  @Override
  protected void addForDeleteInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      addInternal("PRIMARY", new Object[] { file.id }, null);
      addInternal("PID_NAME", new Object[] { file.parentId, file.name }, null);
    }
  }

  @Override
  protected void addInternal(String indexName, Object[] key, Cloneable value) {
    if (indexName.equals("PRIMARY")) {
      DeepArray deepArrayKey = new DeepArray(key.clone());
      CacheValue cacheValue = new CacheValue(indexName, deepArrayKey, value == null ? null : ((File) value).clone());
      cacheOfLog.offer(cacheValue);
      sizeOfLog.getAndIncrement();
      cacheOfId.put(deepArrayKey, cacheValue);
    } else if (indexName.equals("PID_NAME") && key.length == 2) {
      DeepArray deepArrayKey = new DeepArray(key.clone());
      CacheValue cacheValue = new CacheValue(indexName, deepArrayKey, value == null ? null : ((File) value).clone());
      cacheOfLog.offer(cacheValue);
      sizeOfLog.getAndIncrement();
      cacheOfParentIdAndName.put(deepArrayKey, cacheValue);
    }

    if (sizeOfLog.get() > capacity) {
      CacheValue cacheValue = cacheOfLog.poll();
      sizeOfLog.getAndDecrement();
      if (cacheValue != null) {
        if (cacheValue.getName().equals("PRIMARY")) cacheOfId.remove(cacheValue.getKey());
        else if (cacheValue.getName().equals("PID_NAME")) cacheOfParentIdAndName.remove(cacheValue.getKey());
      }
    }
  }

  @Override
  protected void removeInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      cacheOfId.remove(new DeepArray(file.id));
      cacheOfParentIdAndName.remove(new DeepArray(file.parentId, file.name));
    }
  }

  @Override
  protected CacheValue getInternal(boolean updateHitRate, String indexName, Object... key) {
    if (indexName.equals("PRIMARY")) {
      CacheValue cacheValue = cacheOfId.get(new DeepArray(key));
      if (updateHitRate) {
        getNumberOfId.getAndIncrement();
        if (cacheValue != null) hitNumberOfId.getAndIncrement();
      }
      Utilities.logDebug(logger, "threadId=", Thread.currentThread().getId(), "|threadName=", Thread.currentThread()
          .getName(), "|get|", indexName, "|", key, "|", cacheValue, "|hitNumber=", hitNumberOfId.get(), "|getNumber=",
          getNumberOfId.get(), "|hitRate=", hitNumberOfId.get() * 1.0 / getNumberOfId.get());
      return cacheValue;
    } else if (indexName.equals("PID_NAME") && key.length == 2) {
      CacheValue cacheValue = cacheOfParentIdAndName.get(new DeepArray(key));
      if (updateHitRate) {
        getNumberOfParentIdAndName.getAndIncrement();
        if (cacheValue != null) hitNumberOfParentIdAndName.getAndIncrement();
      }
      Utilities.logDebug(logger, "threadId=", Thread.currentThread().getId(), "|threadName=", Thread.currentThread()
          .getName(), "|get|", indexName, "|", key, "|", cacheValue, "|hitNumber=", hitNumberOfParentIdAndName.get(),
          "|getNumber=", getNumberOfParentIdAndName.get(), "|hitRate=", hitNumberOfParentIdAndName.get() * 1.0
              / getNumberOfParentIdAndName.get());
      return cacheValue;
    } else return null;
  }

  @Override
  protected void closeInternal() {
    cacheOfId.clear();
    cacheOfParentIdAndName.clear();
  }

  @Override
  protected String toStringInternal() {
    String string = "cacheOfId-hitNumber=" + hitNumberOfId;
    string += "\ncacheOfId-getNumber=" + getNumberOfId;
    string += "\ncacheOfId-hitRate=" + hitNumberOfId.get() * 100.0F / getNumberOfId.get();
    string += "\ncacheOfId[" + cacheOfId.size() + "]=" + (cacheOfId.size() > 10000 ? "too large" : cacheOfId);
    string += "\ncacheOfParentIdAndName-hitNumber=" + hitNumberOfParentIdAndName;
    string += "\ncacheOfParentIdAndName-getNumber=" + getNumberOfParentIdAndName;
    string +=
        "\ncacheOfParentIdAndName-hitRate=" + hitNumberOfParentIdAndName.get() * 100.0
            / getNumberOfParentIdAndName.get();
    string +=
        "\ncacheOfParentIdAndName[" + cacheOfParentIdAndName.size() + "]="
            + (cacheOfParentIdAndName.size() > 10000 ? "too large" : cacheOfParentIdAndName);
    return string;
  }

  @Override
  protected void updateMetricsInternal(String indexName, Object[] key, CacheValue cacheValue) {
    if (indexName.equals("PRIMARY")) {
      updateMetrics("FileById", cacheOfId, getNumberOfId, hitNumberOfId);
    } else if (indexName.equals("PID_NAME") && key.length == 2) {
      updateMetrics("FileByParentIdAndName", cacheOfParentIdAndName, getNumberOfParentIdAndName,
          hitNumberOfParentIdAndName);
    }
  }

  @Override
  protected void lockForInsertInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      lockInternal("PRIMARY", file.id);
      lockInternal("PID_NAME", file.parentId, file.name);
    }
  }

  @Override
  protected void lockForUpdateInternal(Cloneable oldValue, Cloneable newValue) {
    if (oldValue instanceof File && newValue instanceof File) {
      File oldFile = (File) oldValue;
      File newFile = (File) newValue;
      lockInternal("PRIMARY", newFile.id);
      lockInternal("PID_NAME", newFile.parentId, newFile.name);
      if (newFile.parentId != oldFile.parentId || !newFile.name.equals(oldFile.name))
        lockInternal("PID_NAME", oldFile.parentId, oldFile.name);
    }
  }

  protected void lockForDeleteInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      lockInternal("PRIMARY", file.id);
      lockInternal("PID_NAME", file.parentId, file.name);
    }
  }

  @Override
  protected void lockInternal(String indexName, Object... key) {
    if (indexName.equals("PRIMARY") || (indexName.equals("PID_NAME") && key.length == 2)) {
      locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, indexName, key);
    }
  }

  @Override
  protected boolean tryLockInternal(String indexName, Object... key) {
    if (indexName.equals("PRIMARY") || (indexName.equals("PID_NAME") && key.length == 2))
      return locker.tryLock(null, Long.MAX_VALUE, indexName, key) != null;
    return false;
  }

  @Override
  protected void unlockForInsertInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      unlockInternal("PID_NAME", file.parentId, file.name);
      unlockInternal("PRIMARY", file.id);
    }
  }

  @Override
  protected void unlockForDeleteInternal(Cloneable value) {
    if (value instanceof File) {
      File file = (File) value;
      unlockInternal("PID_NAME", file.parentId, file.name);
      unlockInternal("PRIMARY", file.id);
    }
  }

  @Override
  protected void unlockForUpdateInternal(Cloneable oldValue, Cloneable newValue) {
    if (oldValue instanceof File && newValue instanceof File) {
      File oldFile = (File) oldValue;
      File newFile = (File) newValue;
      if (newFile.parentId != oldFile.parentId || !newFile.name.equals(oldFile.name))
        unlockInternal("PID_NAME", oldFile.parentId, oldFile.name);
      unlockInternal("PID_NAME", newFile.parentId, newFile.name);
      unlockInternal("PRIMARY", newFile.id);
    }
  }

  @Override
  protected void unlockInternal(String indexName, Object... key) {
    if (indexName.equals("PRIMARY") || (indexName.equals("PID_NAME") && key.length == 2)) {
      locker.unlock(null, indexName, key);
    }
  }
}
