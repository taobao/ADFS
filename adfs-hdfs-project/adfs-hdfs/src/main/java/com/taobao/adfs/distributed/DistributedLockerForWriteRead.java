/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.util.DeepArray;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedLockerForWriteRead {
  public static final Logger logger = LoggerFactory.getLogger(DistributedLockerForWriteRead.class);
  Map<DeepArray, ReentrantReadWriteLock> locks = new HashMap<DeepArray, ReentrantReadWriteLock>();

  synchronized public void read(Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    ReentrantReadWriteLock lock = locks.get(lockKey);
    if (lock == null) locks.put(lockKey, lock = new ReentrantReadWriteLock());
    lock.readLock().lock();
  }

  synchronized public void unread(Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    ReentrantReadWriteLock lock = locks.get(new DeepArray(objects));
    if (lock != null) {
      lock.readLock().unlock();
      if (lock.getQueueLength() <= 0 && !lock.isWriteLocked() && lock.getReadLockCount() <= 0) locks.remove(lockKey);
    }
  }

  synchronized public void write(Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    ReentrantReadWriteLock lock = locks.get(lockKey);
    if (lock == null) locks.put(lockKey, lock = new ReentrantReadWriteLock());
    lock.writeLock().lock();
  }

  synchronized public void unwrite(Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    ReentrantReadWriteLock lock = locks.get(new DeepArray(objects));
    if (lock != null) {
      lock.writeLock().unlock();
      if (lock.getQueueLength() <= 0 && !lock.isWriteLocked() && lock.getReadLockCount() <= 0) locks.remove(lockKey);
    }
  }

  public int getSize() {
    return locks.size();
  }

  synchronized public void close() {
    locks.clear();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(DistributedLockerForWriteRead.class.getSimpleName()).append("[").append(locks.size())
        .append("]={");
    for (DeepArray lock : locks.keySet()) {
      stringBuilder.append(lock).insert(stringBuilder.length() - 1, ", " + locks.get(lock)).append(",");
    }
    int indexOfLastChar = stringBuilder.length() - 1;
    if (stringBuilder.charAt(indexOfLastChar) == ',') stringBuilder.deleteCharAt(indexOfLastChar);
    stringBuilder.append("}");
    return stringBuilder.toString();
  }
}
