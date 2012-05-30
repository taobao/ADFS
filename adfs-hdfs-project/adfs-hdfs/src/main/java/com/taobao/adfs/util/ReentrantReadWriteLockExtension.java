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

package com.taobao.adfs.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class ReentrantReadWriteLockExtension extends ReentrantReadWriteLock {
  private static final long serialVersionUID = -4100593776156540976L;
  protected WriteLock writeLock = null;

  public ReentrantReadWriteLockExtension(boolean fair) {
    super(fair);
    writeLock = new WriteLock(this);
  }

  public ReentrantReadWriteLockExtension() {
    this(false);
  }

  @Override
  public WriteLock writeLock() {
    return writeLock;
  }

  @Override
  public String toString() {
    return super.toString() + ", waitingThreadsToLockWrite=" + writeLock.waitingThreadsToLock;
  }

  public class WriteLock extends ReentrantReadWriteLock.WriteLock {
    private static final long serialVersionUID = -4719907993614862239L;
    protected AtomicInteger waitingThreadsToLock = new AtomicInteger(0);

    protected WriteLock(ReentrantReadWriteLock lock) {
      super(lock);
    }

    @Override
    public void lock() {
      waitingThreadsToLock.incrementAndGet();
      try {
        super.lock();
      } finally {
        waitingThreadsToLock.decrementAndGet();
      }
    }

    public int waitingThreadsToLock() {
      return waitingThreadsToLock.get();
    }

    public boolean hasWaitingThreadsToLock() {
      return waitingThreadsToLock() > 0;
    }

    @Override
    public String toString() {
      return super.toString() + ", waitingThreadsToLock=" + waitingThreadsToLock;
    }
  }
}
