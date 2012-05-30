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

package com.taobao.adfs.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class LinkedListAsCpp<E> {
  ListElement head = null;
  ListElement tail = null;
  AtomicInteger size = new AtomicInteger(0);
  Object lockForAddTail = new Object();
  Object lockForRemove = new Object();

  public void addTail(E e) {
    synchronized (lockForAddTail) {
      if (size.get() == 0) {
        head = new ListElement(e);
        tail = head;
      } else {
        tail.setNext(new ListElement(e));
        tail.next.setPrev(tail);
        tail = tail.next();
      }
      size.incrementAndGet();
    }
  }

  public void remove(ListElement le) {
    if (le == null) return;
    if (le == tail || le.next == tail) {
      synchronized (lockForAddTail) {
        removeInternal(le);
      }
    } else removeInternal(le);
  }

  private void removeInternal(ListElement le) {
    synchronized (lockForRemove) {
      size.decrementAndGet();
      if (le == head && le == tail) {
        head = tail = null;
        le.setPrev(null);
        le.setNext(null);
      } else if (le == head && le != tail) {
        head = le.next();
        le.next().setPrev(null);
      } else if (le != head && le == tail) {
        tail = le.prev();
        le.prev().setNext(null);
      } else {
        le.prev().setNext(le.next());
        le.next().setPrev(le.prev());
      }
    }
  }

  public int size() {
    return size.get();
  }

  public synchronized void clear() {
    head = tail = null;
    size.set(0);
  }

  public ListElement head() {
    return head;
  }

  public ListElement tail() {
    return tail;
  }

  public String toString() {
    String str = "size=" + size + ": ";
    ListElement listElement = head();
    while (listElement != null) {
      str += listElement.toString() + "|";
      listElement = listElement.next();
    }
    return str;
  }

  public class ListElement {
    E object = null;
    ListElement prev = null;
    ListElement next = null;

    ListElement(E object) {
      this.object = object;
    }

    public E get() {
      return object;
    }

    public ListElement prev() {
      return prev;
    }

    public ListElement next() {
      return next;
    }

    void setPrev(ListElement le) {
      prev = le;
    }

    void setNext(ListElement le) {
      next = le;
    }

    public String toString() {
      return get() == null ? "null" : Utilities.deepToString(object);
    }
  }
}
