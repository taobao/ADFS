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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.rpc.ObjectWritable;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.DeepArray;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedLocker implements Writable {
  public static final Logger logger = LoggerFactory.getLogger(DistributedLocker.class);
  Map<DeepArray, Object[]> locks = new HashMap<DeepArray, Object[]>();
  DistributedDataVersion version = null;
  boolean isDistributedMode = false;
  Configuration conf = null;
  ReentrantReadWriteLockExtension getDataLocker = null;
  public static Method methodOfLock = null;
  public static Method methodOfTryLock = null;
  public static Method methodOfUnlock = null;
  public static Method methodOfLockDirectly = null;
  public static Method methodOfUnlockDirectly = null;
  static {
    try {
      createMethodForLocker();
    } catch (Throwable t) {
      Utilities.logError(logger, "fail to get methods for distributedLocker", t);
    }
  }

  public DistributedLocker() {
  }

  DistributedLocker(Configuration conf, DistributedDataVersion version, ReentrantReadWriteLockExtension getDataLocker) {
    this.conf = conf == null ? new Configuration(false) : conf;
    this.version = version;
    this.getDataLocker = getDataLocker;
    isDistributedMode = true;
  }

  static void createMethodForLocker() throws IOException {
    if (methodOfLock == null) {
      methodOfLock =
          Invocation.getMethod(DistributedLockerInternalProtocol.class, "lock", String.class, long.class, long.class,
              Object[].class);
      methodOfLock.setAccessible(true);
    }
    if (methodOfTryLock == null) {
      methodOfTryLock =
          Invocation.getMethod(DistributedLockerInternalProtocol.class, "tryLock", String.class, long.class,
              Object[].class);
      methodOfTryLock.setAccessible(true);
    }
    if (methodOfUnlock == null) {
      methodOfUnlock =
          Invocation.getMethod(DistributedLockerInternalProtocol.class, "unlock", String.class, Object[].class);
      methodOfUnlock.setAccessible(true);
    }
    if (methodOfLockDirectly == null) {
      methodOfLockDirectly = Invocation.getMethod(DistributedLocker.class, "lockDirectly", DistributedLock.class);
      methodOfLockDirectly.setAccessible(true);
    }
    if (methodOfUnlockDirectly == null) {
      methodOfUnlockDirectly = Invocation.getMethod(DistributedLocker.class, "unlockDirectly", DistributedLock.class);
      methodOfUnlockDirectly.setAccessible(true);
    }
  }

  public DistributedLock lock(String owner, long expireTime, long timeout, Object... objects) {
    if (owner == null)
      owner = "threadId=" + Thread.currentThread().getId() + "|threadName=" + Thread.currentThread().getName();
    long requestTime = System.currentTimeMillis();
    DeepArray lockKey = new DeepArray(objects);
    while (true) {
      long currentTime = System.currentTimeMillis();
      DistributedLock lock = tryLockInternal(owner, expireTime, currentTime, lockKey);
      if (lock != null) return lock;
      if (currentTime - requestTime > timeout) return null;
      if (getDataLocker != null && getDataLocker.writeLock().hasWaitingThreadsToLock()) {
        lock = getLock(objects);
        if (lock == null || lock.version < 0) continue;
        lock.needRetry = true;
        return lock;
      }
      Utilities.sleepAndProcessInterruptedException(1, logger);
    }
  }

  public DistributedLock tryLock(String owner, long expireTime, Object... objects) {
    if (owner == null)
      owner = "threadId=" + Thread.currentThread().getId() + "|threadName=" + Thread.currentThread().getName();
    return tryLockInternal(owner, expireTime, System.currentTimeMillis(), new DeepArray(objects));
  }

  synchronized private DistributedLock tryLockInternal(String owner, long expireTime, long currentTime,
      DeepArray lockKey) {
    try {
      Object[] lockValue = locks.get(lockKey);
      Invocation currentInvocation = DistributedServer.getCurrentInvocation();
      String operateIdentifier = currentInvocation == null ? null : currentInvocation.getOperateIdentifier();
      if (isDistributedMode && lockValue != null && lockValue[3] != null && lockValue[3].equals(operateIdentifier)) {
        Long oldExpireTimeTo = (Long) lockValue[0];
        Long oldVersion = (Long) lockValue[2];
        return new DistributedLock(lockKey, oldExpireTimeTo, owner, oldVersion, operateIdentifier, false);
      }

      if (lockValue != null && ((Long) lockValue[2]) >= 0 && currentTime < (Long) lockValue[0]) return null;
      else {
        long expireTimeTo = Long.MAX_VALUE - currentTime < expireTime ? Long.MAX_VALUE : currentTime + expireTime;
        long currentVersion = version == null ? 0 : version.increaseAndGet();
        locks.put(lockKey.clone(), new Object[] { expireTimeTo, owner, currentVersion, operateIdentifier });
        return new DistributedLock(lockKey, expireTimeTo, owner, currentVersion, operateIdentifier, false);
      }
    } finally {
      removeExpiredLock();
    }
  }

  public DistributedLock unlock(String owner, Object... objects) {
    if (owner == null)
      owner = "threadId=" + Thread.currentThread().getId() + "|threadName=" + Thread.currentThread().getName();
    return unlockInternal(owner, objects);
  }

  synchronized private DistributedLock unlockInternal(String owner, Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    Object[] lockValue = locks.get(lockKey);
    Invocation currentInvocation = DistributedServer.getCurrentInvocation();
    String operateIdentifier = currentInvocation == null ? null : currentInvocation.getOperateIdentifier();
    if (isDistributedMode && lockValue != null && lockValue[3] != null && lockValue[3].equals(operateIdentifier)) {
      Long oldExpireTimeTo = (Long) lockValue[0];
      Long oldVersion = (Long) lockValue[2];
      return new DistributedLock(lockKey, oldExpireTimeTo, owner, oldVersion, operateIdentifier, false);
    }

    if (lockValue != null && ((Long) lockValue[2]) >= 0 && owner.equals((String) lockValue[1])) {
      if (version != null) lockValue[2] = -version.increaseAndGet();
      lockValue[3] = operateIdentifier;
      locks.remove(lockKey);
      return new DistributedLock(lockKey, (Long) lockValue[0], owner, (Long) lockValue[2], (String) lockValue[3], false);
    } else {
      Utilities.logWarn(logger, owner, " fails to unlock lockKey=", Utilities.deepToString(objects), ", lockValue=",
          lockValue, ", exception=", new Throwable());
      return null;
    }
  }

  public DistributedLock getLock(Object... objects) {
    DeepArray lockKey = new DeepArray(objects);
    Object[] lockValue = locks.get(lockKey);
    if (lockValue == null) return null;
    return new DistributedLock(lockKey, (Long) lockValue[0], (String) lockValue[1], (Long) lockValue[2],
        (String) lockValue[3], false);
  }

  synchronized public DistributedLock lockDirectly(DistributedLock lock) {
    try {
      if (lock == null || lock.lockKey == null) return null;
      Object[] lockValue = locks.get(lock.lockKey);
      if (lockValue == null || Math.abs((Long) lockValue[2]) <= Math.abs(lock.version)) {
        locks.put(lock.lockKey, new Object[] { lock.expireTimeTo, lock.owner, lock.version, lock.operateIdentifier });
      } else if (Math.abs((Long) lockValue[2]) < 0) locks.remove(lock.lockKey);
      return lock;
    } finally {
      removeExpiredLock();
    }
  }

  public AtomicInteger removeCount = new AtomicInteger(0);

  public void removeExpiredLock() {
    long currentTime = System.currentTimeMillis();
    if (conf == null) return;
    long removeExpiredLockInterval = conf.getInt("distributed.locker.remove.expired.lock.interval", 100);
    if (removeExpiredLockInterval < 1) removeExpiredLockInterval = 1;
    if (version != null && removeCount.getAndIncrement() % removeExpiredLockInterval == 0) {
      long removeExpiredLockDelayTime = conf.getInt("distributed.locker.remove.expired.lock.delay.time", 600000);
      synchronized (this) {
        Iterator<Entry<DeepArray, Object[]>> lockEntryIterator = locks.entrySet().iterator();
        while (lockEntryIterator.hasNext()) {
          Entry<DeepArray, Object[]> lockEntry = lockEntryIterator.next();
          Object[] lockValue = lockEntry.getValue();
          Long expireTimeTo = (Long) lockValue[0];
          Long version = (Long) lockValue[2];
          if (version < 0 && currentTime > expireTimeTo + removeExpiredLockDelayTime) {
            lockEntryIterator.remove();
            Utilities.logInfo(logger, "remove expired lock|key=", lockEntry.getKey(), "|value=", lockEntry.getValue());
          }
        }
      }
    }
  }

  synchronized public DistributedLock unlockDirectly(DistributedLock lock) {
    if (lock == null || lock.lockKey == null) return null;
    Object[] lockValue = locks.get(lock.lockKey);
    if (lockValue == null || Math.abs((Long) lockValue[2]) <= Math.abs(lock.version)) {
      // lock->unlock===>unlock->lock
      locks.put(lock.lockKey, new Object[] { lock.expireTimeTo, lock.owner, lock.version, lock.operateIdentifier });
    }
    version.greaterAndSet(Math.abs(lock.version));
    return lock;
  }

  public int getSize() {
    return locks.size();
  }

  synchronized public void set(DistributedLocker locker) {
    locks.clear();
    if (locker == null) return;
    locks.putAll(locker.locks);
  }

  synchronized public void close() {
    if (locks != null) locks.clear();
  }

  synchronized public DistributedLocker clone() {
    DistributedLocker locker = new DistributedLocker();
    locker.set(this);
    return locker;
  }

  @Override
  synchronized public void write(DataOutput out) throws IOException {
    out.writeInt(locks.size());
    for (DeepArray lockKey : locks.keySet()) {
      ObjectWritable.writeArray(out, lockKey.getObjects());
      Object[] lockValue = locks.get(lockKey);
      out.writeLong((Long) lockValue[0]);
      ObjectWritable.writeString(out, (String) lockValue[1]);
      out.writeLong((Long) lockValue[2]);
      ObjectWritable.writeString(out, (String) lockValue[3]);
    }
  }

  @Override
  synchronized public void readFields(DataInput in) throws IOException {
    locks.clear();
    int size = in.readInt();
    for (int i = 0; i < size; ++i) {
      DeepArray lockKey = new DeepArray((Object[]) ObjectWritable.readArray(in, Object[].class));
      Object[] lockValue =
          new Object[] { in.readLong(), ObjectWritable.readString(in), in.readLong(), ObjectWritable.readString(in) };
      locks.put(lockKey, lockValue);
    }
  }

  protected boolean isTooLargeForToString() {
    return 1024 * locks.size() * 2 > Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory();
  }

  @Override
  public String toString() {
    try {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(DistributedLocker.class.getSimpleName()).append("[").append(locks.size()).append("]={");
      if (isTooLargeForToString()) stringBuilder.append("too large");
      else {
        synchronized (this) {
          for (DeepArray lockKey : locks.keySet()) {
            Object[] lockValue = locks.get(lockKey);
            String expireTimeTo = Utilities.longTimeToStringTime((Long) lockValue[0], "");
            String owner = (String) lockValue[1];
            Long version = (Long) lockValue[2];
            String operateIdentifier = (String) lockValue[3];
            stringBuilder.append(lockKey).deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("|expireTime=").append(expireTimeTo);
            stringBuilder.append("|owner=").append(owner);
            stringBuilder.append("|version=").append(version);
            stringBuilder.append("|operateIdentifier=").append(operateIdentifier);
            stringBuilder.append("],");
          }
        }
      }
      int indexOfLastChar = stringBuilder.length() - 1;
      if (stringBuilder.charAt(indexOfLastChar) == ',') stringBuilder.deleteCharAt(indexOfLastChar);
      stringBuilder.append("}");
      return stringBuilder.toString();
    } catch (Throwable t) {
      // java.lang.OutOfMemoryError will be thrown when element number is large
      return Utilities.deepToString("FAIL TO CALL toString(): " + t);
    }
  }

  static public class DistributedLock extends DistributedResult implements Writable {
    DeepArray lockKey = null;
    long expireTimeTo = Long.MIN_VALUE;
    String owner = null;
    long version = Long.MIN_VALUE;
    String operateIdentifier = null;

    public DistributedLock() {
      super(false);
    }

    public DistributedLock(DeepArray lock, long expireTimeTo, String owner, long version, String operateIdentifier,
        boolean needRetry) {
      super(needRetry);
      this.lockKey = lock;
      this.expireTimeTo = expireTimeTo;
      this.owner = owner;
      this.version = version;
      this.operateIdentifier = operateIdentifier;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (expireTimeTo ^ (expireTimeTo >>> 32));
      result = prime * result + ((lockKey == null) ? 0 : lockKey.hashCode());
      result = prime * result + ((operateIdentifier == null) ? 0 : operateIdentifier.hashCode());
      result = prime * result + ((owner == null) ? 0 : owner.hashCode());
      result = prime * result + (int) (version ^ (version >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      DistributedLock other = (DistributedLock) obj;
      if (expireTimeTo != other.expireTimeTo) return false;
      if (lockKey == null) {
        if (other.lockKey != null) return false;
      } else if (!lockKey.equals(other.lockKey)) return false;
      if (operateIdentifier == null) {
        if (other.operateIdentifier != null) return false;
      } else if (!operateIdentifier.equals(other.operateIdentifier)) return false;
      if (owner == null) {
        if (other.owner != null) return false;
      } else if (!owner.equals(other.owner)) return false;
      if (version != other.version) return false;
      return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      ObjectWritable.writeArray(out, lockKey.getObjects());
      out.writeLong(expireTimeTo);
      ObjectWritable.writeString(out, owner);
      out.writeLong(version);
      ObjectWritable.writeString(out, operateIdentifier);
      out.writeBoolean(needRetry);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      lockKey = new DeepArray((Object[]) ObjectWritable.readArray(in, Object[].class));
      expireTimeTo = in.readLong();
      owner = ObjectWritable.readString(in);
      version = in.readLong();
      operateIdentifier = ObjectWritable.readString(in);
      needRetry = in.readBoolean();
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      if (lockKey == null) stringBuilder.append("[lockKey=null");
      else {
        String lockKeyString = lockKey.toString();
        lockKeyString = lockKeyString.substring(1, lockKeyString.length() - 1);
        stringBuilder.append("[lockKey=(").append(lockKeyString).append(")");
      }
      stringBuilder.append("|expireTime=").append(expireTimeTo);
      stringBuilder.append("|owner=").append(owner);
      stringBuilder.append("|version=").append(version);
      stringBuilder.append("|operateIdentifier=").append(operateIdentifier);
      stringBuilder.append("|needRetry=").append(needRetry);
      stringBuilder.append("]");
      return stringBuilder.toString();
    }
  }
}
