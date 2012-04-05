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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedLocker.DistributedLock;
import com.taobao.adfs.distributed.rpc.ObjectWritable;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public abstract class DistributedData implements DistributedLockerInternalProtocol, Writable {
  public static final Logger logger = LoggerFactory.getLogger(DistributedData.class);
  public Configuration conf = null;
  protected DistributedDataVersion version = new DistributedDataVersion();
  protected DistributedLocker distributedLocker = null;
  protected ReentrantReadWriteLockExtension getDataLocker = null;

  public DistributedData() {
  }

  public DistributedData(ReentrantReadWriteLockExtension getDataLocker) {
    this.getDataLocker = getDataLocker;
  }

  /**
   * all writable elements in elementsToTransfer will be serialized and deserialized during RPC call
   */
  private Map<String, Object> elementsToTransfer = new HashMap<String, Object>();

  protected void initialize() throws IOException {
    try {
      open();
      if (conf.getBoolean("distributed.data.format", false)) {
        format();
        conf.setBoolean("distributed.data.format", false);
      }
      conf.setBoolean("distributed.data.initialize.result", true);
      Utilities.logInfo(logger, "succeed in initializing data");
    } catch (Throwable t) {
      conf.setBoolean("distributed.data.initialize.result", false);
      Utilities.logWarn(logger, "fail to initialize data", t);
      setDataVersion(-1);
    }
  }

  private void createDataPath() throws IOException {
    File dataFile = new File(getDataPath());
    try {
      dataFile.mkdirs();
    } catch (Throwable t) {
      throw new IOException("fail to create data path=" + dataFile.getAbsolutePath(), t);
    }
  }

  public String getDataPath() {
    return (String) elementsToTransfer.get("distributed.data.path");
  }

  synchronized public void open() throws IOException {
    String normalDataPath = Utilities.getNormalPath(conf.get("distributed.data.path", "."));
    if (!normalDataPath.equals(conf.get("distributed.data.path"))) {
      conf.set("distributed.data.path", normalDataPath);
      Utilities.logDebug(logger, "data path=", conf.get("distributed.data.path"));
    }
    openElementToTransfer("distributed.data.path", conf.get("distributed.data.path"));
    openElementToTransfer("distributed.server.name", conf.get("distributed.server.name"));
    openElementToTransfer("distributed.data.restore.increment.enable", conf.getLong(
        "distributed.data.restore.increment.version.gap.max", -1) > -1);
    createDataPath();
    if (distributedLocker == null) distributedLocker = new DistributedLocker(conf, version, getDataLocker);
  }

  public boolean isValid() {
    return false;
  }

  void openElementToTransfer(String key, Object defaultValue) throws IOException {
    if (elementsToTransfer.get(key) != null) return;

    Object newValue = null;
    if (defaultValue == null) newValue = conf.get(key);
    else if (defaultValue.getClass().equals(Boolean.class)) newValue = conf.getBoolean(key, (Boolean) defaultValue);
    else if (defaultValue.getClass().equals(Integer.class)) newValue = conf.getInt(key, (Integer) defaultValue);
    else if (defaultValue.getClass().equals(Long.class)) newValue = conf.getLong(key, (Long) defaultValue);
    else if (defaultValue.getClass().equals(String.class)) newValue = conf.get(key, (String) defaultValue);
    else throw new IOException("unsupported type of element to transfer");

    if (newValue != null) conf.set(key, newValue.toString());
    elementsToTransfer.put(key, newValue);
  }

  synchronized public void close() throws IOException {
    if (distributedLocker != null) distributedLocker.close();
  }

  synchronized public void backup() throws IOException {
  }

  synchronized public Object[] format() throws IOException {
    setDataVersion(0);
    setIsInIncrementRestoreStage(false, false);
    return new Object[0];
  }

  public Object[] formatDirectly(Object[] objects) throws IOException {
    return format();
  }

  public long getDataVersion() {
    if (conf != null) return version.get();
    else return (Long) getElementToTransfer("distributed.data.version");
  }

  public long setDataVersion(long newVersion) throws IOException {
    if (conf != null) version.set(newVersion);
    else putElementToTransfer("distributed.data.version", newVersion);
    return newVersion;
  }

  /**
   * 1.elements size; 2.all key-value pairs, for any key-value pair, read key first and then value;
   */
  public void readFields(DataInput in) throws IOException {
    elementsToTransfer.clear();
    int elementSize = in.readInt();
    for (int i = 0; i < elementSize; ++i) {
      String key = ObjectWritable.readString(in);
      Object value = ObjectWritable.readObject(in, null);
      elementsToTransfer.put(key, value);
      Utilities.logDebug(logger, "read elementsToTransfer with key=", key);
    }
  }

  /**
   * 1.elements size; 2.all key-value pairs, for any key-value pair, write key first and then value;
   */
  public void write(DataOutput out) throws IOException {
    elementsToTransfer.put("distributed.data.version", getDataVersion());
    Set<String> writableElementKeys = new HashSet<String>();
    for (String key : elementsToTransfer.keySet()) {
      if (ObjectWritable.isWritable(elementsToTransfer.get(key))) writableElementKeys.add(key);
    }
    out.writeInt(writableElementKeys.size());
    for (String key : writableElementKeys) {
      ObjectWritable.writeString(out, key);
      ObjectWritable.writeObject(out, elementsToTransfer.get(key), Object.class);
      Utilities.logDebug(logger, "write elementsToTransfer with key=", key);
    }
  }

  public Object getElementToTransfer(String key) {
    return elementsToTransfer.get(key);
  }

  public Object putElementToTransfer(String key, Object value) {
    return elementsToTransfer.put(key, value);
  }

  public Object removeElementToTransfer(String key) {
    return elementsToTransfer.remove(key);
  }

  public Class<?>[] getDataProtocols() {
    return getClass().getInterfaces();
  }

  @Override
  public DistributedLock lock(String owner, long expireTime, long timeout, Object... objects) throws IOException {
    return distributedLocker.lock(owner, expireTime, timeout, objects);
  }

  @Override
  public DistributedLock tryLock(String owner, long expireTime, Object... objects) throws IOException {
    return distributedLocker.tryLock(owner, expireTime, objects);
  }

  @Override
  public DistributedLock unlock(String owner, Object... objects) throws IOException {
    return distributedLocker.unlock(owner, objects);
  }

  synchronized public DistributedData getData(DistributedData oldData,
      ReentrantReadWriteLockExtension.WriteLock writeLock) throws IOException {
    DistributedData newData = null;
    if (oldData.getIsIncrementRestoreEnabled()) newData = getDataIncrement(oldData, writeLock);
    else newData = getDataAll(oldData, writeLock);

    String serverName = conf.get("distributed.server.name");
    String remoteServerName = (String) oldData.getElementToTransfer("distributed.server.name");
    if (this != oldData) {
      Utilities.logInfo(logger, serverName, " start to get distributedDataLocker for ", remoteServerName);
      DistributedLocker newDataLocker = getDataLocker().clone();
      newData.putElementToTransfer("distributed.data.locker", newDataLocker);
      Utilities.logInfo(logger, serverName, " succeed in getting distributedDataLocker for ", remoteServerName,
          ", size=", newDataLocker.getSize());
      Utilities.logDebug(logger, "get ", newDataLocker);
    }

    return newData;
  }

  public DistributedData getDataAll(DistributedData oldData, ReentrantReadWriteLockExtension.WriteLock writeLock)
      throws IOException {
    throw new IOException("need to implement this function in your data");
  }

  public DistributedData getDataIncrement(DistributedData oldData, ReentrantReadWriteLockExtension.WriteLock writeLock)
      throws IOException {
    throw new IOException("need to implement this function in your data");
  }

  public void setData(DistributedData newData) throws IOException {
    if (getIsIncrementRestoreEnabled()) setDataIncrement(newData);
    else setDataAll(newData);
  }

  public void setDataAll(DistributedData newData) throws IOException {
    throw new IOException("need to implement this function in your data");
  }

  public void setDataIncrement(DistributedData newData) throws IOException {
    throw new IOException("need to implement this function in your data");
  }

  public Object invoke(Invocation invocation) throws IOException {
    if (invocation.getResult() == null) {
      if (invocation.getMethod().equals(DistributedLocker.methodOfLock)
          || invocation.getMethod().equals(DistributedLocker.methodOfTryLock)
          || invocation.getMethod().equals(DistributedLocker.methodOfUnlock)) {
        invocation.getParameters()[0] = invocation.getCallerName(null, null, true);
      }
      return invocation.invoke(this);
    } else {
      long startTime = System.currentTimeMillis();
      Invocation directInvocation = getDirectInvocation(invocation);
      if (directInvocation == null) return invocation.invoke(this);
      invocation.setResult(directInvocation.invoke());
      invocation.setProxyInvocation(directInvocation);
      invocation.setElapsedTime(System.currentTimeMillis() - startTime);
      return invocation.getResult();
    }
  }

  public Invocation getDirectInvocation(Invocation invocation) throws IOException {
    if (invocation.getMethodName().equals("format")) {
      return new Invocation(this, "formatDirectly", invocation.getResult());
    } else if (invocation.getMethod().equals(DistributedLocker.methodOfLock)
        || invocation.getMethod().equals(DistributedLocker.methodOfTryLock)) {
      return new Invocation(distributedLocker, DistributedLocker.methodOfLockDirectly, invocation.getResult());
    } else if (invocation.getMethod().equals(DistributedLocker.methodOfUnlock)) {
      return new Invocation(distributedLocker, DistributedLocker.methodOfUnlockDirectly, invocation.getResult());
    } else return null;
  }

  public boolean getIsInIncrementRestoreStage(boolean onlyReadFromMemory) throws IOException {
    if (!onlyReadFromMemory) {
      File fileForInIncrementRestoreStage = new File(getDataPath(), "inIncrementRestoreStage");
      conf.setBoolean("distributed.data.restore.increment.stage", fileForInIncrementRestoreStage.isFile());
    }
    return conf.getBoolean("distributed.data.restore.increment.stage", false);
  }

  public void setIsInIncrementRestoreStage(boolean isInIncrementRestoreStage, boolean onlyWriteToMemory)
      throws IOException {
    if (!onlyWriteToMemory) {
      File fileForInIncrementRestoreStage = new File(getDataPath(), "inIncrementRestoreStage");
      fileForInIncrementRestoreStage.getParentFile().mkdirs();
      boolean lastStatusOfIsInIncrementRestoreStage = getIsInIncrementRestoreStage(false);
      Utilities.delete(fileForInIncrementRestoreStage);
      if (isInIncrementRestoreStage) {
        fileForInIncrementRestoreStage.createNewFile();
        Utilities.logInfo(logger, "create inIncrementRestoreStage file=", fileForInIncrementRestoreStage
            .getAbsolutePath(), "|last status=", lastStatusOfIsInIncrementRestoreStage);
      } else {
        Utilities.logInfo(logger, "delete inIncrementRestoreStage file=", fileForInIncrementRestoreStage
            .getAbsolutePath(), "|last status=", lastStatusOfIsInIncrementRestoreStage);
      }
    }
    conf.setBoolean("distributed.data.restore.increment.stage", isInIncrementRestoreStage);
  }

  public boolean getIsIncrementRestoreEnabled() {
    return (Boolean) elementsToTransfer.get("distributed.data.restore.increment.enable");
  }

  public void setIsIncrementRestoreEnabled(boolean isIncrementRestoreEnabled) {
    elementsToTransfer.put("distributed.data.restore.increment.enable", isIncrementRestoreEnabled);
  }

  public DistributedLocker getDataLocker() {
    return distributedLocker;
  }

  public DistributedLocker setDataLocker(DistributedLocker newDistributedDataLocker) {
    return distributedLocker = newDistributedDataLocker;
  }

  static Map<Method, Class<?>> distributedOperationTypeCache = new HashMap<Method, Class<?>>();

  public static Class<?> getDistributedInvocationType(Method method) {
    Class<?> operationType = distributedOperationTypeCache.get(method);
    if (operationType != null) return operationType;
    for (Annotation annotation : method.getAnnotations()) {
      operationType = annotation.annotationType();
      if (operationType == DistributedRead.class || operationType == DistributedWrite.class) {
        distributedOperationTypeCache.put(method, operationType);
        return operationType;
      }
    }
    return null;
  }

  public static String getDataClientClassName(String dataClassName) {
    if (dataClassName == null) return null;
    String[] classNameParts = dataClassName.split("\\.");
    if (classNameParts.length < 3) return dataClassName;
    if (!classNameParts[classNameParts.length - 2].equals("internal")) return dataClassName;
    String dataClassSimpleName = classNameParts[classNameParts.length - 1];
    if (!dataClassSimpleName.endsWith("Internal")) return dataClassName;
    String dataClientClassSimpleName =
        dataClassSimpleName.substring(0, dataClassSimpleName.length() - "internal".length());
    if (dataClientClassSimpleName.isEmpty()) return dataClassName;
    String dataClientClassPackageName = "";
    for (int i = 0; i < classNameParts.length - 2; ++i) {
      dataClientClassPackageName += classNameParts[i] + ".";
    }
    return dataClientClassPackageName + dataClientClassSimpleName;
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  public @interface DistributedRead {
  }

  @Retention(RUNTIME)
  @Target(METHOD)
  public @interface DistributedWrite {
  }
}
