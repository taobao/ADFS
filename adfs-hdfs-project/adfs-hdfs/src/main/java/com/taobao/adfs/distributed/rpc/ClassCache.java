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

package com.taobao.adfs.distributed.rpc;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class ClassCache {
  static Map<String, Class<?>> classCache = new ConcurrentHashMap<String, Class<?>>();
  static Map<Class<?>, Field[]> declaredFieldCache = new ConcurrentHashMap<Class<?>, Field[]>();
  static Map<Class<?>, Field[]> allFieldCache = new ConcurrentHashMap<Class<?>, Field[]>();
  static {
    classCache.put("boolean", Boolean.TYPE);
    classCache.put("byte", Byte.TYPE);
    classCache.put("char", Character.TYPE);
    classCache.put("short", Short.TYPE);
    classCache.put("int", Integer.TYPE);
    classCache.put("long", Long.TYPE);
    classCache.put("float", Float.TYPE);
    classCache.put("double", Double.TYPE);
    classCache.put("void", Void.TYPE);
  }

  /**
   * comparing Class.forName(String className), performance is 7~8 times
   */
  public static Class<?> get(String name) throws ClassNotFoundException {
    Class<?> clazz = classCache.get(name);
    if (clazz == null) {
      clazz = Class.forName(name);
      classCache.put(name, clazz);
    }
    return clazz;
  }

  public static Class<?> getWithIOException(String name) throws IOException {
    try {
      return get(name);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  public static Field[] getDeclaredFields(Class<?> clazz) {
    Field[] fields = declaredFieldCache.get(clazz);
    if (fields == null) {
      List<Field> fieldList = new ArrayList<Field>();
      for (Field field : clazz.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers())) {
          field.setAccessible(true);
          fieldList.add(field);
        }
      }
      fields = fieldList.toArray(new Field[fieldList.size()]);
      declaredFieldCache.put(clazz, fields);
    }
    return fields;
  }

  public static Field[] getAllFields(Class<?> clazz) {
    Field[] fields = allFieldCache.get(clazz);
    if (fields == null) {
      List<Field> fieldList = new ArrayList<Field>();
      try {
        for (Field field : Utilities.getFields(clazz)) {
          if (!Modifier.isStatic(field.getModifiers())) {
            field.setAccessible(true);
            fieldList.add(field);
          }
        }
      } catch (Throwable t) {
        fields = null;
      }
      fields = fieldList.toArray(new Field[fieldList.size()]);
      allFieldCache.put(clazz, fields);
    }
    return fields;
  }

  public static void performance(int repeat) throws ClassNotFoundException {
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < repeat; ++i) {
      get(ClassCache.class.getName());
    }
    long t1 = System.currentTimeMillis();
    System.out.println("get class in cache, OPS=" + repeat * 1000.0 / (t1 - t0) + "/S");

    t0 = System.currentTimeMillis();
    for (int i = 0; i < repeat; ++i) {
      Class.forName(ClassCache.class.getName());
    }
    t1 = System.currentTimeMillis();
    System.out.println("get class no cache, OPS=" + repeat * 1000.0 / (t1 - t0) + "/S");
  }
}
