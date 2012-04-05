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

package com.taobao.adfs.distributed.rpc;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Arrays;

import java.io.*;

import org.apache.hadoop.io.Writable;

import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class ObjectWritable implements Writable {
  private Class<?> declaredClass;
  private Object instance;

  public ObjectWritable() {
  }

  public ObjectWritable(Object instance) {
    set(instance);
  }

  public ObjectWritable(Class<?> declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public Object get() {
    return instance;
  }

  /** Return the class this is meant to be. */
  public Class<?> getDeclaredClass() {
    return declaredClass;
  }

  /** Reset the instance. */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }

  public String toString() {
    return "ObjectWritable[class=" + declaredClass + ",value=" + instance + "]";
  }

  public void readFields(DataInput in) throws IOException {
    readObject(in, this);
  }

  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass);
  }

  private static class NullInstance {
  }

  /**
   * Write a {@link Writable}, {@link String}, primitive type, or an array of the preceding.
   */
  public static void writeObject(DataOutput out, Object instance, Class<?> declaredClass) throws IOException {
    if (instance == null) instance = new NullInstance();
    Class<?> realClass = instance.getClass();
    writeString(out, declaredClass.getName());
    writeString(out, realClass.equals(declaredClass) ? "" : realClass.getName());

    if (realClass == NullInstance.class) {
      // nothing to do
    } else if (realClass.isArray()) {
      writeArray(out, instance);
    } else if (realClass == String.class) {
      writeString(out, (String) instance);
    } else if (Utilities.isNativeOrJavaPrimitiveType(realClass)) {
      writeNativeOrJavaPrimitiveType(out, instance);
    } else if (realClass.isEnum()) {
      writeString(out, ((Enum<?>) instance).name());
    } else if (Writable.class.isAssignableFrom(realClass)) {
      ((Writable) instance).write(out);
    } else if (realClass.isAssignableFrom(Class.class)) {
      writeClass(out, (Class<?>) instance);
    } else if (instance instanceof AutoWritable || realClass.getSuperclass().equals(Object.class)) {
      writeAutomatically(out, instance);
    } else {
      throw new IOException("Can't write: " + instance + " as " + realClass);
    }
  }

  /**
   * Read a {@link Writable}, {@link String}, primitive type, or an array of the preceding.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static Object readObject(DataInput in, ObjectWritable objectWritable) throws IOException {
    Class<?> declaredClass = null;
    Class<?> realClass = null;
    String declaredClassName = null;
    String realClassName = null;
    try {
      declaredClassName = readString(in);
      declaredClass = ClassCache.get(declaredClassName);
      realClassName = readString(in);
      realClass = realClassName.isEmpty() ? declaredClass : ClassCache.get(realClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("declaredClassName=" + declaredClassName + ", realClassName=" + realClassName, e);
    }

    Object instance = null;
    if (realClass == NullInstance.class) {
      instance = null;
    } else if (realClass.isArray()) {
      instance = readArray(in, realClass);
    } else if (realClass == String.class) {
      instance = readString(in);
    } else if (Utilities.isNativeOrJavaPrimitiveType(realClass)) {
      instance = readNativeOrJavaPrimitiveType(in, realClass);
    } else if (realClass.isEnum()) {
      instance = Enum.valueOf((Class<? extends Enum>) realClass, readString(in));
    } else if (Writable.class.isAssignableFrom(realClass)) {
      instance = WritableFactories.newInstance((Class<? extends Writable>) realClass);
      ((Writable) instance).readFields(in);
    } else if (Class.class.isAssignableFrom(realClass)) {
      instance = readClass(in);
    } else if (AutoWritable.class.isAssignableFrom(realClass.getSuperclass())
        || realClass.getSuperclass().equals(Object.class)) {
      instance = readAutomatically(in, realClass);
    } else {
      throw new IOException("Can't read: " + instance + " as " + realClass);
    }

    if (objectWritable != null) { // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }

    return instance;
  }

  public static short hexByteArrayToShort(byte[] hexByteArray) throws IOException {
    if (hexByteArray == null || hexByteArray.length != 4) throw new IOException("incorrect hex byte array");
    try {
      return (short) Integer.parseInt(new String(hexByteArray), 16);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static byte[] shortToHexByteArray(short value) throws IOException {
    try {
      return Arrays.copyOfRange(Integer.toHexString(value & 0x0000FFFF | 0x000F0000).getBytes(), 1, 4);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static void writeHexShort(DataOutput out, short value) throws IOException {
    out.write(shortToHexByteArray(value));
  }

  public static short readHexShort(DataInput in) throws IOException {
    byte[] lengthBytes = new byte[4];
    in.readFully(lengthBytes);
    return hexByteArrayToShort(lengthBytes);
  }

  public static void writeString(DataOutput out, String string) throws IOException {
    if (string == null) out.writeInt(-1);
    else {
      byte[] bytes = string.getBytes();
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  public static String readString(DataInput in) throws IOException {
    int length = in.readInt();
    if (length < 0) return null;
    byte[] stringBytes = new byte[length];
    in.readFully(stringBytes);
    return new String(stringBytes);
  }

  public static void writeClass(DataOutput out, Class<?> clazz) throws IOException {
    writeString(out, clazz.getName());
  }

  public static Class<?> readClass(DataInput in) throws IOException {
    try {
      return ClassCache.get(readString(in));
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  public static void writeArray(DataOutput out, Object object) throws IOException {
    int length = Array.getLength(object);
    out.writeInt(length);
    for (int i = 0; i < length; i++) {
      writeObject(out, Array.get(object, i), object.getClass().getComponentType());
    }
  }

  public static Object readArray(DataInput in, Class<?> realClass) throws IOException {
    int length = in.readInt();
    Object object = Array.newInstance(realClass.getComponentType(), length);
    for (int i = 0; i < length; i++) {
      Array.set(object, i, readObject(in, null));
    }
    return object;
  }

  public static void writeAutomatically(DataOutput out, Object object) throws IOException {
    try {
      for (Field field : ClassCache.getAllFields(object.getClass())) {
        ObjectWritable.writeObject(out, field.get(object), field.getType());
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static Object readAutomatically(DataInput in, Class<?> realClass) throws IOException {
    try {
      Object object = realClass.newInstance();
      for (Field field : ClassCache.getAllFields(realClass)) {
        field.set(object, ObjectWritable.readObject(in, null));
      }
      return object;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static boolean isWritable(Object object) {
    if (object == null) return true;
    Class<?> clazz = object.getClass();
    if (clazz == Class.class) return true;
    if (clazz == String.class) return true;
    if (clazz.isEnum()) return true;
    if (Utilities.isNativeOrJavaPrimitiveType(clazz)) return true;
    if (Writable.class.isAssignableFrom(clazz)) {
      if (object instanceof ObjectWritable) return isWritable(((ObjectWritable) object).instance);
      return true;
    }
    if (clazz.isArray()) {
      int length = Array.getLength(object);
      for (int i = 0; i < length; i++) {
        if (!isWritable(Array.get(object, i))) return false;
      }
      return true;
    }
    if (object instanceof AutoWritable || clazz.getSuperclass().equals(Object.class)) {
      for (Field field : ClassCache.getAllFields(object.getClass())) {
        try {
          if (!isWritable(field.get(object))) return false;
        } catch (Throwable t) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * write native primitive or java primitive type
   */
  public static void writeNativeOrJavaPrimitiveType(DataOutput out, Object object) throws IOException {
    Class<?> realClass = object.getClass();
    if (realClass == Boolean.TYPE || realClass == Boolean.class) {
      out.writeBoolean((Boolean) object);
    } else if (realClass == Character.TYPE || realClass == Character.class) {
      out.writeChar((Character) object);
    } else if (realClass == Byte.TYPE || realClass == Byte.class) {
      out.writeByte((Byte) object);
    } else if (realClass == Short.TYPE || realClass == Short.class) {
      out.writeShort((Short) object);
    } else if (realClass == Integer.TYPE || realClass == Integer.class) {
      out.writeInt((Integer) object);
    } else if (realClass == Long.TYPE || realClass == Long.class) {
      out.writeLong((Long) object);
    } else if (realClass == Float.TYPE || realClass == Float.class) {
      out.writeFloat((Float) object);
    } else if (realClass == Double.TYPE || realClass == Double.class) {
      out.writeDouble((Double) object);
    } else if (realClass == Void.TYPE || realClass == Void.class) {
    } else {
      throw new IOException("Not a native or java primitive: " + realClass);
    }
  }

  /**
   * read native primitive or java primitive type
   */
  public static Object readNativeOrJavaPrimitiveType(DataInput in, Class<?> realClass) throws IOException {
    if (realClass == Boolean.TYPE || realClass == Boolean.class) {
      return Boolean.valueOf(in.readBoolean());
    } else if (realClass == Character.TYPE || realClass == Character.class) {
      return Character.valueOf(in.readChar());
    } else if (realClass == Byte.TYPE || realClass == Byte.class) {
      return Byte.valueOf(in.readByte());
    } else if (realClass == Short.TYPE || realClass == Short.class) {
      return Short.valueOf(in.readShort());
    } else if (realClass == Integer.TYPE || realClass == Integer.class) {
      return Integer.valueOf(in.readInt());
    } else if (realClass == Long.TYPE || realClass == Long.class) {
      return Long.valueOf(in.readLong());
    } else if (realClass == Float.TYPE || realClass == Float.class) {
      return Float.valueOf(in.readFloat());
    } else if (realClass == Double.TYPE || realClass == Double.class) {
      return Double.valueOf(in.readDouble());
    } else if (realClass == Void.TYPE || realClass == Void.class) {
      return null;
    } else {
      throw new IllegalArgumentException("Not a native or java primitive: " + realClass);
    }
  }
}
