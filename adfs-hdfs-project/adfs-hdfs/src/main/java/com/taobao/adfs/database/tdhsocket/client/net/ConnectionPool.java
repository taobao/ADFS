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

package com.taobao.adfs.database.tdhsocket.client.net;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author <a href="mailto:wentong@taobao.com">文通</a>
 * @since 11-12-26 下午3:55
 */
public class ConnectionPool<T> {
    private List<T> pool;

    private int index = 0;

    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ConnectionPool(int num) {
        pool = new ArrayList<T>(num);
    }

    public void add(T t, Handler<T> handler) {
        rwLock.writeLock().lock();
        try {
            if (handler != null) {
                handler.execute(t);
            }
            pool.add(t);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public boolean remove(T t) {
        rwLock.writeLock().lock();
        try {
            return pool.remove(t);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Method get ...
     *
     * @return Channel
     */
    public T get() {
        rwLock.readLock().lock();
        try {
            if (pool.isEmpty()) {
                return null;
            }
            int idx = index++ % pool.size();
            return pool.get(idx);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void close(Handler<T> handler) {
        rwLock.writeLock().lock();
        try {
            Iterator<T> iterator = pool.iterator();
            while (iterator.hasNext()) {
                T t = iterator.next();
                if (handler != null) {
                    handler.execute(t);
                }
                iterator.remove();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public int size() {
        rwLock.readLock().lock();
        try {
            return pool.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        rwLock.readLock().lock();
        try {
            return pool.isEmpty();
        } finally {
            rwLock.readLock().unlock();
        }
    }


    public interface Handler<T> {
        void execute(T t);
    }


}
