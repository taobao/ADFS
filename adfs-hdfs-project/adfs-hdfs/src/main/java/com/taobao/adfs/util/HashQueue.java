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

package com.taobao.adfs.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class HashQueue<E> implements Queue<E> {
  LinkedList<E> list = null;
  HashSet<E> set = null;
  int capacity = Integer.MAX_VALUE;

  public HashQueue() {
    this(Integer.MAX_VALUE);
  }

  public HashQueue(int capacity) {
    list = new LinkedList<E>();
    set = new HashSet<E>();
    if (capacity < 0) capacity = 0;
    this.capacity = capacity;
  }

  @Override
  public int size() {
    return set.size();
  }

  public int capacity() {
    return capacity;
  }

  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  public boolean isFull() {
    return size() >= capacity;
  }

  @Override
  public boolean contains(Object o) {
    return set.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return set.iterator();
  }

  @Override
  public Object[] toArray() {
    return set.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return set.toArray(a);
  }

  @Override
  public boolean remove(Object o) {
    synchronized (list) {
      list.remove(o);
      return set.remove(o);
    }
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return set.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    if (c == null) throw new NullPointerException();
    synchronized (list) {
      if (capacity - size() < c.size())
        throw new IllegalStateException("not enough space, size=" + set.size() + ", capacity=" + capacity
            + ", requestSize=" + c.size());
      for (E e : c) {
        if (e == null) throw new NullPointerException();
      }
      list.addAll(c);
      return set.addAll(c);
    }
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    synchronized (list) {
      list.removeAll(c);
      return set.removeAll(c);
    }
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return set.retainAll(c);
  }

  @Override
  public void clear() {
    synchronized (list) {
      list.clear();
      set.clear();
    }
  }

  @Override
  public boolean add(E e) {
    if (e == null) throw new NullPointerException();
    synchronized (list) {
      if (set.contains(e)) return false;
      if (capacity - size() < 1)
        throw new IllegalStateException("not enough space, size=" + set.size() + ", capacity=" + capacity
            + ", requestSize=" + 1);
      list.addLast(e);
      return set.add(e);
    }
  }

  @Override
  public boolean offer(E e) {
    if (e == null) throw new NullPointerException();
    synchronized (list) {
      if (set.contains(e)) return false;
      if (capacity - size() < 1)
        throw new IllegalStateException("not enough space, size=" + set.size() + ", capacity=" + capacity
            + ", requestSize=" + 1);
      list.addLast(e);
      set.add(e);
      return true;
    }
  }

  @Override
  public E remove() {
    synchronized (list) {
      E e = list.remove();
      if (e == null) return null;
      set.remove(e);
      return e;
    }
  }

  @Override
  public E poll() {
    synchronized (list) {
      E e = list.poll();
      if (e == null) return null;
      set.remove(e);
      return e;
    }
  }

  @Override
  public E element() {
    return list.element();
  }

  @Override
  public E peek() {
    return list.peek();
  }
}
