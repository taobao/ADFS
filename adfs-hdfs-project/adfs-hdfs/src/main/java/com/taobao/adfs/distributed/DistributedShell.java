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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Level;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedManager.ServerStatuses;
import com.taobao.adfs.distributed.DistributedServer.ServerStatus;
import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.ClassCache;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public class DistributedShell {
  public static final Logger logger = LoggerFactory.getLogger(DistributedShell.class);

  public static void main(String[] args) throws IOException, IllegalArgumentException, IllegalAccessException,
      InvocationTargetException, SecurityException, InstantiationException, NoSuchMethodException,
      ClassNotFoundException {
    DistributedShell distrbutedShell = new DistributedShell(args);
    if (!distrbutedShell.execute()) distrbutedShell.printHelp();
    distrbutedShell.close();
  }

  String[] args = null;
  Configuration conf = null;
  Closeable dataClient = null;

  DistributedShell(String[] args) throws IOException, IllegalArgumentException, SecurityException,
      ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException,
      NoSuchMethodException {
    this.args = Utilities.parseVmArgs(args, null);
    conf = Utilities.loadConfiguration("distributed-server");
    configLogger(conf);
  }

  void configLogger(Configuration conf) throws IOException {
    Utilities.configureLog4j(conf, "distributed.logger.conf.", Level.DEBUG);
    Utilities.setLoggerLevel(ZooKeeper.class.getName(), Level.ERROR.toString(), null);
    Utilities.setLoggerLevel(ClientCnxn.class.getName(), Level.ERROR.toString(), null);
    Utilities.setLoggerLevel(NIOServerCnxn.class.getName(), Level.ERROR.toString(), null);
    Utilities.setLoggerLevel(PrepRequestProcessor.class.getName(), Level.ERROR.toString(), null);
    Utilities.setLoggerLevel(Utilities.class.getName(), Level.WARN.toString(), null);
    Utilities.setLoggerLevel(DistributedClient.class.getName(), Level.WARN.toString(), null);
    Utilities.setLoggerLevel(DistributedMetrics.class.getName(), Level.WARN.toString(), null);
    Utilities.setConfDefaultValue(conf, "distributed.logger.follow.excludes", Utilities.class.getName() + ","
        + DistributedClient.class.getName() + "," + DistributedMetrics.class.getName());
    Utilities.setLoggerLevel(conf, null);
  }

  boolean execute() throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException,
      InvocationTargetException, NoSuchMethodException, IOException {
    if (shellForStopServers(args)) return true;
    if (shellForGetServers(args)) return true;
    if (shellForMonitor(args)) return true;
    if (shellForClient(args)) return true;
    return false;
  }

  Closeable getDataClient() {
    if (dataClient != null) return dataClient;
    if (conf.getBoolean("distributed.shell.get.data.client.has.been.invoke", false)) return null;
    conf.setBoolean("distributed.shell.get.data.client.has.been.invoke", true);
    try {
      String clientClassName = conf.get("distributed.data.client.class.name");
      if (clientClassName == null) {
        try {
          String dataClassName = DistributedClient.getDistributedDataTypeName(conf);
          clientClassName = DistributedData.getDataClientClassName(dataClassName);
          if (clientClassName.equals(dataClassName)) {
            clientClassName = null;
            conf.set("distributed.data.class.name", dataClassName);
          } else clientClassName = ClassCache.get(clientClassName).getName();
        } catch (ClassNotFoundException e) {
          clientClassName = null;
        }
      }

      if (clientClassName == null || clientClassName.isEmpty()) {
        dataClient = DistributedClient.getClient(conf);
      } else {
        dataClient = (Closeable) ClassCache.get(clientClassName).getConstructor(Configuration.class).newInstance(conf);
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
    return dataClient;
  }

  boolean shellForClient(String[] args) throws IllegalArgumentException, SecurityException, InstantiationException,
      IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
    if (args.length == 0 || getDataClient() == null) return false;
    String[] argsForMethod = (String[]) getSubArray(args, 1, args.length - 1);
    String methodName = args[0];
    if (methodName.contains(".")) methodName = methodName.split("\\.", 2)[1];
    invokeMethod(dataClient, methodName, argsForMethod, true);
    return conf.getBoolean("distributed.shell.invoke.method.is.called", false);
  }

  boolean shellForMonitor(String[] args) throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException, IOException, SecurityException, InstantiationException, NoSuchMethodException {
    if (args.length < 2) return false;
    DistributedMonitor distributedMonitor = new DistributedMonitor(conf, args[0]);
    String[] argsForMethod = (String[]) getSubArray(args, 2, args.length - 2);
    invokeMethod(distributedMonitor, args[1], argsForMethod, false);
    return conf.getBoolean("distributed.shell.invoke.method.is.called", false);
  }

  Object[] getSubArray(Object[] array, int offset, int length) {
    Object[] subArray = (Object[]) Array.newInstance(array.getClass().getComponentType(), length);
    for (int i = 0; i < length; ++i) {
      subArray[i] = array[i + offset];
    }
    return subArray;
  }

  Object invokeMethod(Object object, String name, String[] parameterPairs, boolean allowDefault) throws IOException,
      IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    // find method
    conf.setBoolean("distributed.shell.invoke.method.is.called", false);
    Method[] methodMatched = findMethod(object.getClass(), name);
    if (methodMatched.length == 0) return null;
    String[] parameterTypeSimpleNames = new String[parameterPairs.length];
    String[] paramterStringValues = new String[parameterPairs.length];
    if (!parseParameterTypesAndValues(parameterPairs, parameterTypeSimpleNames, paramterStringValues)) return null;
    methodMatched = findMethod(methodMatched, name, parameterTypeSimpleNames);
    if (methodMatched.length == 0) return null;
    conf.setBoolean("distributed.shell.invoke.method.is.called", true);
    if (methodMatched.length > 1) {
      System.out.println("matched more than one method, need to specify more arguments:");
      printHelpForMethods(methodMatched, null, name);
      return null;
    }

    // invoke method
    Class<?>[] parameterTypes = methodMatched[0].getParameterTypes();
    if (!allowDefault && parameterPairs.length < parameterTypes.length) {
      System.out.println("need to specify more arguments: ");
      printHelpForMethods(methodMatched, null, name);
      return null;
    }
    Object[] methodArgs = new Object[methodMatched[0].getParameterTypes().length];
    for (int i = 0; i < paramterStringValues.length; ++i) {
      methodArgs[i] = stringToObject(paramterStringValues[i], parameterTypes[i]);
    }
    Invocation invocation = new Invocation(methodMatched[0], methodArgs);
    invocation.invoke(object);
    System.out.println("call " + invocation.toString(true));
    return invocation.getResult();
  }

  static boolean parseParameterTypesAndValues(String[] parameterPairs, String[] parameterTypeSimpleNames,
      String[] parameterValueStrings) {
    for (int i = 0; i < parameterPairs.length; ++i) {
      String[] parameterPair = parameterPairs[i].split("=", 2);
      if (parameterPair.length < 2) {
        System.out.println("fail to parse parameter=[" + parameterPair[i] + "], format should be [type=value]");
        return false;
      }
      parameterTypeSimpleNames[i] = parameterPair[0];
      parameterValueStrings[i] = parameterPair[1];
    }
    return true;
  }

  Method[] findMethod(Class<?> clazz, String name) {
    List<Method> methods = new ArrayList<Method>();
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name)) methods.add(method);
    }
    return methods.toArray(new Method[methods.size()]);
  }

  static Method[] findMethod(Class<?> clazz, String name, String[] parameterSimpleTypes) {
    return findMethod(clazz.getMethods(), name, parameterSimpleTypes);
  }

  static Method[] findMethod(Method[] methods, String name, String[] parameterSimpleTypes) {
    // get matched methods and least parameter number
    List<Method> methodsMatched = new ArrayList<Method>();
    int leastParameterNumber = Integer.MAX_VALUE;
    for (Method method : methods) {
      Class<?>[] parameterTypes = method.getParameterTypes();
      if (method.getName().equals(name) && parameterSimpleTypes.length <= parameterTypes.length) {
        // check parameter type
        boolean matched = true;
        for (int i = 0; i < parameterSimpleTypes.length; ++i) {
          if (!parameterTypes[i].getSimpleName().equals(parameterSimpleTypes[i])) {
            matched = false;
            break;
          }
        }
        if (matched) {
          methodsMatched.add(method);
          if (leastParameterNumber > parameterTypes.length) leastParameterNumber = parameterTypes.length;
        }
      }
    }
    // get method with least parameters
    List<Method> methodMatchedWithLeastParameters = new ArrayList<Method>();
    for (Method method : methodsMatched) {
      int parameterTypesLength = method.getParameterTypes().length;
      if (leastParameterNumber == parameterTypesLength) methodMatchedWithLeastParameters.add(method);
    }
    return methodMatchedWithLeastParameters.toArray(new Method[methodMatchedWithLeastParameters.size()]);
  }

  boolean shellForStopServers(String[] args) throws IOException {
    if (args.length == 0 || args.length > 2 || !args[0].equals("stop")) return false;

    List<ServerStatus> serverStatusList = new ArrayList<ServerStatus>();
    if (args.length == 2) serverStatusList.add(new ServerStatus(args[1], null, null, null, null, null, null));
    else {
      ServerStatuses serverStatuses = new DistributedManager(conf).getServers(null);
      serverStatusList.addAll(serverStatuses.getNot(false, ServerType.STOP));
      ServerStatus.sortByTypeAndName(serverStatusList);
    }

    if (serverStatusList.isEmpty()) System.out.println("no server was found");
    for (int i = serverStatusList.size() - 1; i >= 0; --i) {
      System.out.println(serverStatusList.get(i).name + " is stopping");
      new DistributedMonitor(conf, serverStatusList.get(i).name).stop();
      System.out.println(serverStatusList.get(i).name + " is stopped");
    }
    return true;
  }

  boolean shellForGetServers(String[] args) throws IOException {
    if (args.length != 1 || !args[0].equals("getServers")) return false;
    DistributedManager distributedManager = new DistributedManager(conf);
    List<ServerStatus> serverStatusList = distributedManager.getServers(null).getNotStop(false);
    ServerStatus.sortByTypeAndName(serverStatusList);
    String dataType = distributedManager.getName();
    String result = serverStatusList.isEmpty() ? "no server was found" : "";
    for (ServerStatus serverStatus : serverStatusList) {
      DistributedMonitor distributedMonitor = new DistributedMonitor(conf, serverStatus.name);
      String serverType = "unknown";
      String serverVersion = "unknown";
      String serverPid = "unknown";
      try {
        serverType = distributedMonitor.getServerType().toString();
        serverVersion = String.valueOf(distributedMonitor.getDataVersion());
        serverPid = String.valueOf(distributedMonitor.getServerPid());
      } catch (Throwable t) {
        // ignore it
      }
      result += serverStatus.name;
      result += "|Data=" + dataType;
      result += "|Type=" + serverType;
      result += "|Version=" + serverVersion;
      result += "|Pid=" + serverPid + " ";
    }
    System.out.println(result);
    return true;
  }

  void printHelp() {
    printHelpForClient();
    printHelpForMonitor();
    printHelpForShell();
  }

  void printHelpForClient() {
    if (getDataClient() == null) return;
    Class<?>[] interfaces = dataClient.getClass().getInterfaces();
    printHelpForClass(interfaces[0], null, null);
  }

  void printHelpForMonitor() {
    printHelpForClass(DistributedMonitor.class, "serverName ", null);
  }

  void printHelpForShell() {
    System.out.println("---------------------------" + getClass().getSimpleName() + " help---------------------------");
    System.out.println("stop [serverName]");
    System.out.println("getServers");
  }

  void printHelpForClass(Class<?> clazz, String prefix, String name) {
    System.out.println("---------------------------" + clazz.getSimpleName() + " help---------------------------");
    if (clazz.isInterface()) printHelpForMethods(clazz.getMethods(), prefix, name);
    else printHelpForMethods(clazz.getDeclaredMethods(), prefix, name);
  }

  void printHelpForMethods(Method[] methods, String prefix, String name) {
    // sort by name
    for (int i = 0; i < methods.length - 1; ++i) {
      for (int j = i + 1; j < methods.length; ++j) {
        if (methods[j].getName().compareTo(methods[i].getName()) < 0) {
          Method method = methods[j];
          methods[j] = methods[i];
          methods[i] = method;
        }
      }
    }
    // print help
    for (Method method : methods) {
      if (!Modifier.isPublic(method.getModifiers())) continue;
      if (name != null && !name.isEmpty() && !name.equals(method.getName())) continue;
      String typeInfo = "";
      for (Class<?> type : method.getParameterTypes()) {
        typeInfo += " " + type.getSimpleName();
      }
      if (prefix != null && !prefix.isEmpty()) System.out.print(prefix);
      System.out.println(method.getName() + typeInfo);
    }
  }

  static Object stringToObject(String value, Class<?> newType) throws IOException {
    if (newType.equals(String.class)) {
      return value;
    } else if (newType.equals(Boolean.class) || newType.equals(boolean.class)) {
      return Boolean.valueOf(value);
    } else if (newType.equals(Byte.class) || newType.equals(byte.class)) {
      return Byte.valueOf(value);
    } else if (newType.equals(Character.class) || newType.equals(char.class)) {
      return new Character(value.charAt(0));
    } else if (newType.equals(Short.class) || newType.equals(short.class)) {
      return Short.valueOf(value);
    } else if (newType.equals(Integer.class) || newType.equals(int.class)) {
      return Integer.valueOf(value);
    } else if (newType.equals(Long.class) || newType.equals(long.class)) {
      return Long.valueOf(value);
    } else if (newType.equals(Float.class) || newType.equals(float.class)) {
      return Float.valueOf(value);
    } else if (newType.equals(Double.class) || newType.equals(double.class)) {
      return Double.valueOf(value);
    } else if (newType.equals(ServerType.class)) {
      return ServerType.valueOf(value);
    } else if (newType.equals(FsPermission.class)) {
      if (value.length() < 4) return new FsPermission(Short.valueOf(value));
      else return new FsPermission(Short.valueOf(value, 8));
    } else if (newType.equals(Object.class) && value.equalsIgnoreCase("null")) {
      return null;
    } else {
      try {
        return newType.getConstructor(String.class).newInstance(value);
      } catch (Throwable t) {
      }
      try {
        Object newObject = newType.newInstance();
        String[] fieldValues = value.split(",");
        Field[] fields = newType.getDeclaredFields();
        int valueIndex = 0;
        for (Field field : fields) {
          if (Modifier.isStatic(field.getModifiers())) continue;
          field.setAccessible(true);
          if (valueIndex >= fieldValues.length) break;
          field.set(newObject, stringToObject(fieldValues[valueIndex++], field.getType()));
        }
        return newObject;
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }
  }

  public void close() throws IOException {
    if (dataClient != null) dataClient.close();
  }
}
