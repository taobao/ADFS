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

package com.taobao.adfs.distributed;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.util.Cloneable;
import com.taobao.adfs.util.DeepArray;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DistributedDataCache {
  public static final Logger logger = LoggerFactory.getLogger(DistributedDataCache.class);
  protected Queue<CacheValue> cacheOfLog = new ConcurrentLinkedQueue<CacheValue>();
  protected AtomicInteger sizeOfLog = new AtomicInteger(0);
  protected DistributedLocker locker = new DistributedLocker();

  protected int capacity = 0;

  protected DistributedDataCache(int capacity) {
    if (capacity > 0) this.capacity = capacity;
  }

  protected abstract void lockInternal(String indexName, Object... key);

  protected abstract boolean tryLockInternal(String indexName, Object... key);

  protected abstract void lockForInsertInternal(Cloneable value);

  protected abstract void lockForDeleteInternal(Cloneable value);

  protected abstract void lockForUpdateInternal(Cloneable oldValue, Cloneable newValue);

  protected abstract void unlockInternal(String indexName, Object... key);

  protected abstract void unlockForDeleteInternal(Cloneable value);

  protected abstract void unlockForInsertInternal(Cloneable value);

  protected abstract void unlockForUpdateInternal(Cloneable oldValue, Cloneable newValue);

  protected abstract void addInternal(String indexName, Object[] key, Cloneable value);

  protected abstract void addForInsertInternal(Cloneable value);

  protected abstract void addForDeleteInternal(Cloneable value);

  protected abstract void addForUpdateInternal(Cloneable oldValue, Cloneable newValue);

  protected abstract CacheValue getInternal(boolean updateHitRate, String indexName, Object... key);

  protected abstract void removeInternal(Cloneable value);

  protected abstract void updateMetricsInternal(String indexName, Object[] key, CacheValue cacheValue);

  protected abstract void closeInternal();

  protected abstract String toStringInternal();

  public void lockForUpdate(Cloneable oldValue, Cloneable newValue) {
    if (oldValue == null || oldValue == null || newValue == null) return;
    lockForUpdateInternal(oldValue, newValue);
  }

  public void lockForInsert(Cloneable value) {
    if (value == null) return;
    lockForInsertInternal(value);
  }

  public void lockForDelete(Cloneable value) {
    if (value == null) return;
    lockForDeleteInternal(value);
  }

  public boolean lockForFind(String indexName, Object[] key) {
    if (indexName == null || key == null) return false;
    return tryLockInternal(indexName, key);
  }

  public void unlockForInsert(Cloneable value) {
    if (value == null) return;
    unlockForInsertInternal(value);
  }

  public void unlockForDelete(Cloneable value) {
    if (value == null) return;
    unlockForDeleteInternal(value);
  }

  public void unlockForFind(String indexName, Object[] key) {
    if (indexName == null || key == null) return;
    unlockInternal(indexName, key);
  }

  public void unlockForUpdate(Cloneable oldValue, Cloneable newValue) {
    if (oldValue == null || newValue == null) return;
    unlockForUpdateInternal(oldValue, newValue);
  }

  public int getCapacity() {
    return capacity;
  }

  public void addForInsert(Cloneable value) {
    if (capacity <= 0 || value == null) return;
    addForInsertInternal(value);
  }

  protected void addForFind(String indexName, Object[] key, List<?> values) {
    if (capacity <= 0 || values == null || indexName == null || key == null) return;
    if (values.isEmpty()) addInternal(indexName, key, null);
    else {
      Object value = values.get(0);
      if (value instanceof Cloneable) addInternal(indexName, key, (Cloneable) value);
    }
  }

  public void addForUpdate(Cloneable oldValue, Cloneable newValue) {
    if (capacity <= 0 || oldValue == null || newValue == null) return;
    addForUpdateInternal(oldValue, newValue);
  }

  public void addForDelete(Cloneable value) {
    if (capacity <= 0 || value == null) return;
    addForDeleteInternal(value);
  }

  public void remove(Cloneable... values) {
    if (capacity <= 0 || values == null) return;
    for (Cloneable value : values) {
      if (value != null) removeInternal(value);
    }
  }

  public CacheValue get(boolean updateHitRate, String indexName, Object... key) {
    CacheValue cacheValue = getInternal(updateHitRate, indexName, key);
    updateMetrics(indexName, key, cacheValue);
    return cacheValue == null ? null : cacheValue.clone();
  }

  public void close() {
    closeInternal();
    cacheOfLog.clear();
    locker.close();
  }

  public String toString() {
    try {
      String string = getClass().getSimpleName() + "={";
      string += "\ncapacity=" + capacity;
      string += "\nlocker=" + locker;
      string += "\n" + toStringInternal();
      string += "\ncacheOfLog[" + sizeOfLog.get() + "]=" + (sizeOfLog.get() > 10000 ? "too large" : cacheOfLog);
      string += "\n}";
      return string;
    } catch (Throwable t) {
      // java.lang.OutOfMemoryError will be thrown when element number is large
      return Utilities.deepToString("FAIL TO CALL toString(): " + t);
    }
  }

  protected void updateMetrics(String indexName, Object[] key, CacheValue cacheValue) {
    updateMetricsInternal(indexName, key, cacheValue);
    DistributedMetrics.intValueaSet("datacache.sizeOfLog", sizeOfLog.get());
  }

  protected void updateMetrics(String cacheName, Map<?, ?> cacheMap, AtomicLong getNumber, AtomicLong hitNumber) {
    // when DistributedDataCache is being created, cacheMap/getNumber/hitNumber has not been initialized
    if (cacheMap == null || getNumber == null || hitNumber == null) return;
    int hitRate = getNumber.get() == 0 ? 0 : Math.round(hitNumber.get() * 100.0F / getNumber.get());
    DistributedMetrics.longValueaSet("datacache.get" + cacheName + "_hit_number", hitNumber.get());
    DistributedMetrics.longValueaSet("datacache.get" + cacheName + "_get_number", getNumber.get());
    DistributedMetrics.intValueaSet("datacache.get" + cacheName + "_hit_rate", hitRate);
    DistributedMetrics.intValueaSet("datacache.get" + cacheName + "_size", cacheMap.size());
  }

  public class CacheValue implements Cloneable {
    String name = null;
    DeepArray key = null;
    Cloneable value = null;

    public CacheValue(String name, DeepArray key, Cloneable value) {
      if (name == null) throw new RuntimeException("name is null");
      if (key == null) throw new RuntimeException("key is null");
      this.name = name;
      this.key = key;
      this.value = value;
    }

    public String getName() {
      return name;
    }

    public DeepArray getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public CacheValue clone() {
      return new CacheValue(name, key.clone(), value == null ? null : value.clone());
    }

    @Override
    public String toString() {
      return "{name=" + name + ",key=" + key + ",value=" + value + "}";
    }
  }
}
