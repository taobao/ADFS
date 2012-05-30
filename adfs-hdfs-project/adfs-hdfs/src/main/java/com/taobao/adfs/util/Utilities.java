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

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.rpc.ClassCache;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class Utilities {
  public static final Logger logger = LoggerFactory.getLogger(Utilities.class);
  static String currentPath = null;

  /**
   * useful when current path includes a symbolic link
   */
  public static String getCurrentPath() throws IOException {
    if (currentPath == null) currentPath = runCommand("pwd", null, null, null);
    return currentPath;
  }

  public static String getLoggerLevel(String loggerName) throws IOException {
    Level level = LogManager.getLogger(loggerName).getLevel();
    return level == null ? null : level.toString();
  }

  public static List<String> getLoggerInfos() throws IOException {
    List<String> stringListOfloggers = new ArrayList<String>();
    stringListOfloggers.add(LogManager.getRootLogger().getName());
    Enumeration<?> loggers = LogManager.getLoggerRepository().getCurrentLoggers();
    while (loggers.hasMoreElements()) {
      org.apache.log4j.Logger logger = (org.apache.log4j.Logger) loggers.nextElement();
      String nameAndLevel = logger.getName() + "=" + getLoggerLevel(logger.getName());
      stringListOfloggers.add(nameAndLevel);
    }
    return stringListOfloggers;
  }

  public static boolean stringStartsWith(String string, String... strings) {
    if (string == null || strings == null) return false;
    for (String tempString : strings) {
      if (tempString == null || tempString.isEmpty()) continue;
      if (string.startsWith(tempString)) return true;
    }
    return false;
  }

  public static boolean stringEquals(String string, String... strings) {
    if (strings == null) return false;
    for (String tempString : strings) {
      if (string == null && tempString == null) return true;
      if (string != null && string.equals(tempString)) return true;
    }
    return false;
  }

  public static List<org.apache.log4j.Logger> getLoggers() throws IOException {
    List<org.apache.log4j.Logger> listOfloggers = new ArrayList<org.apache.log4j.Logger>();
    listOfloggers.add(LogManager.getRootLogger());
    Enumeration<?> loggers = LogManager.getLoggerRepository().getCurrentLoggers();
    while (loggers.hasMoreElements()) {
      org.apache.log4j.Logger logger = (org.apache.log4j.Logger) loggers.nextElement();
      listOfloggers.add(logger);
    }
    return listOfloggers;
  }

  public static String getNormalPath(String path) throws IOException {
    if (!path.startsWith("/")) path = getCurrentPath() + "/" + path;
    path = new File(path).getAbsolutePath();
    path = path.replaceAll("\\.\\.", "**");
    path = path.replaceAll("\\." + File.pathSeparator + "+", "");
    while (true) {
      int posOfParentPath = path.indexOf("**");
      if (posOfParentPath < 0) break;
      int posOfSeparator = path.lastIndexOf(File.separator, posOfParentPath - 2);
      if (posOfSeparator < 0) posOfSeparator = 0;
      path = path.substring(0, posOfSeparator) + path.substring(posOfParentPath + 2);
      if (path.isEmpty()) path = File.separator;
    }

    return path;
  }

  /**
   * overwrite properties with system properties
   */
  public static Configuration loadConfiguration(Configuration conf) {
    if (conf == null) return null;
    for (Object key : System.getProperties().keySet()) {
      Object value = System.getProperty((String) key);
      if (key instanceof String && value instanceof String) conf.set((String) key, (String) value);
    }
    return conf;
  }

  public static String[] loadConfiguration(Configuration conf, String[] args) {
    if (conf == null) return null;
    args = parseVmArgs(args, null);
    loadConfiguration(conf);
    return args;
  }

  public static Configuration loadConfiguration(String prefix) {
    if (System.getProperty("conf.prefix") != null) prefix = System.getProperty("conf.prefix");
    if (prefix == null) prefix = "";
    Configuration conf = new Configuration(false);
    System.setProperty("conf", System.getProperty("conf", "conf") + ":" + getLibPath().getParent() + "/conf");
    List<File> confFiles = getConfFile(null, prefix + "-default.xml");
    confFiles.addAll(getConfFile(null, prefix + "-site.xml"));
    for (File confFile : confFiles) {
      conf.addResource(new Path(confFile.getPath()));
      logDebug(logger, "add conf file=", confFile);
    }
    if (confFiles.isEmpty()) logDebug(logger, "no conf file is added");

    return loadConfiguration(conf);
  }

  public static String[] parseArgsToVmArgs(String[] args, String prefixOfVmArgs) {
    if (prefixOfVmArgs == null) return args;
    args = args.clone();
    List<String> argList = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      if (args[i] != null && !args[i].startsWith("-D")) {
        if (args[i].startsWith("--")) args[i] = args[i].substring(2);
        if (args[i].startsWith("-")) args[i] = args[i].substring(1);
        args[i] = prefixOfVmArgs + args[i] + "=";
        if ((i + 1) < args.length && !args[i + 1].startsWith("-")) argList.add(args[i] += args[++i]);
        else argList.add(args[i]);
      } else if (args[i] != null) argList.add(args[i]);
      else continue;
    }
    return argList.toArray(new String[argList.size()]);
  }

  public static String[] parseVmArgs(String[] args, String prefixOfVmArgs) {
    args = parseArgsToVmArgs(args, prefixOfVmArgs);
    if (args == null) return null;
    List<String> newArgs = new ArrayList<String>();
    for (String arg : args) {
      if (arg.startsWith("-D") && arg.contains("=")) {
        String[] propertyPair = arg.substring(2).split("=", 2);
        System.setProperty(propertyPair[0], propertyPair[1]);
      } else newArgs.add(arg);
    }
    return newArgs.toArray(new String[newArgs.size()]);
  }

  public static Map<String, String> getConf(Configuration conf, String prefix) throws IOException {
    Map<String, String> confMap = new HashMap<String, String>();
    if (conf == null) return confMap;
    Properties properties = (Properties) new Invocation(conf.getClass(), "getProps").invoke(conf);
    for (String key : properties.stringPropertyNames()) {
      if (prefix == null || key.startsWith(prefix)) confMap.put(key, conf.get(key));
    }
    return confMap;
  }

  /**
   * return file or child file with specified postfix
   */
  public static File getFile(String path, String postfix) {
    if (path == null || postfix == null) return null;
    File file = new File(path);
    if (file.isFile()) {
      if (file.getName().endsWith(postfix)) return file;
      else return null;
    } else if (file.isDirectory()) {
      for (File childFile : file.listFiles()) {
        if (childFile.isFile() && childFile.getName().endsWith(postfix)) return childFile;
      }
      return null;
    } else return null;
  }

  public static List<File> getConfFile(String path, String postfix) {
    if (path == null) path = "";
    path += ":" + System.getProperty("conf", "conf");
    path += ":" + getLibPath().getParent() + "/conf";
    String[] confPaths = path.split(":");
    List<File> confFiles = new ArrayList<File>();
    for (int i = confPaths.length - 1; i >= 0; --i) {
      File confFile = getFile(confPaths[i], postfix);
      boolean isAdded = false;
      if (confFile != null) {
        for (File addedConfFile : confFiles) {
          if (addedConfFile.getPath().equals(confFile.getPath())) isAdded = true;
        }
        if (!isAdded) confFiles.add(confFile);
      }
    }
    return confFiles;
  }

  /**
   * is native primitive or java primitive type
   */
  public static boolean isNativeOrJavaPrimitiveType(Object object) {
    return object == null ? false : isNativeOrJavaPrimitiveType(object.getClass());
  }

  /**
   * is native primitive or java primitive type
   */
  public static boolean isNativeOrJavaPrimitiveType(Class<?> clazz) {
    if (clazz.isPrimitive()) return true;
    if (clazz.equals(Boolean.class) || clazz.equals(Character.class) || clazz.equals(Byte.class)
        || clazz.equals(Short.class) || clazz.equals(Integer.class) || clazz.equals(Long.class)
        || clazz.equals(Float.class) || clazz.equals(Double.class) || clazz.equals(Void.class)) return true;
    return false;
  }

  public static void addResource(URLClassLoader classLoader, File file) throws IOException {
    try {
      Method addURL = URLClassLoader.class.getDeclaredMethod("addURL", new Class[] { URL.class });
      addURL.setAccessible(true);
      addURL.invoke(classLoader, new Object[] { file.toURI().toURL() });
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static void setLoggerLevel(Configuration conf, Logger logger) throws IOException {
    String[] loggerLevels = conf.get("distributed.logger.levels", "").trim().split(",");
    for (String loggerLevel : loggerLevels) {
      String[] loggerLevelPair = loggerLevel.split("=");
      if (loggerLevelPair.length != 2) continue;
      setLoggerLevel(loggerLevelPair[0], loggerLevelPair[1], logger);
    }
  }

  public static String setLoggerLevel(String loggerName, String level, Logger logger) throws IOException {
    Level newLevel = (level == null || level.equals("null")) ? null : Level.toLevel(level);
    Level oldLevel = LogManager.getLogger(loggerName).getLevel();
    if ((newLevel == null && oldLevel == null) || (newLevel != null && newLevel.equals(oldLevel))) return level;
    log(logger, Level.INFO, "set logger level: ", loggerName, "=", newLevel);
    LogManager.getLogger(loggerName).setLevel(newLevel);
    return level;
  }

  public static boolean sleepAndProcessInterruptedException(long millis, Logger logger) {
    try {
      Thread.sleep(millis);
      return true;
    } catch (InterruptedException e) {
      if (logger != null) logError(logger, "thread is interrupted", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public static <T> String addString(List<T> objectList) {
    StringBuilder stringBuilder = new StringBuilder(objectList.size() * 32);
    for (Object object : objectList) {
      stringBuilder.append(deepToString(object));
    }
    return stringBuilder.toString();
  }

  public static String toStringByFields(Object object, boolean allFields) {
    if (object == null) return "null";
    StringBuilder stringBuilder = new StringBuilder(256);
    stringBuilder.append(object.getClass().getSimpleName()).append('[');
    Field[] fields =
        allFields ? ClassCache.getAllFields(object.getClass()) : ClassCache.getDeclaredFields(object.getClass());
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i].getAnnotation(IgnoreToString.class) != null) continue;
      fields[i].setAccessible(true);
      if (Modifier.isStatic(fields[i].getModifiers())) continue;
      stringBuilder.append(fields[i].getName()).append('=');
      try {
        Object fieldValue = fields[i].get(object);
        if (fieldValue == null) fieldValue = "null";
        else {
          String timeFormat = getTimeFormatFromField(fields[i]);
          if (timeFormat != null) fieldValue = longTimeToStringTime((Long) fieldValue, timeFormat);
        }
        stringBuilder.append(deepToString(fieldValue));
      } catch (Throwable t) {
        stringBuilder.append("unknown");
        logWarn(logger, "fail to get string for ", fields[i].getName(), t);
      }
      if (i < fields.length - 1) stringBuilder.append(',');
    }
    stringBuilder.append(']');
    return stringBuilder.toString();
  }

  public static String getTimeFormatFromField(Field field) {
    if (field == null) return null;
    if (!field.getType().equals(long.class) && !field.getType().equals(Long.class)) return null;
    for (Annotation annotation : field.getAnnotations()) {
      if (annotation instanceof LongTime) return ((LongTime) annotation).value();
    }
    return null;
  }

  @Retention(RUNTIME)
  public @interface LongTime {
    String value();
  }

  @Retention(RUNTIME)
  public @interface IgnoreToString {
  }

  public static String longTimeToStringTime(long time, String format) {
    if (format.isEmpty()) format = "yyyyMMdd-HHmmss-SSS";
    return new SimpleDateFormat(format).format(new Date(time));
  }

  public static long stringTimeToLongTime(String time, String format) throws IOException {
    if (format.isEmpty()) format = "yyyyMMdd-HHmmss-SSS";
    try {
      return new SimpleDateFormat(format).parse(time).getTime();
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  public static String getCurrentTime() {
    return longTimeToStringTime(System.currentTimeMillis(), "");
  }

  public static String deepToString(Object object) {
    if (object == null) {
      return "null";
    } else if (object instanceof Throwable) {
      return getThrowableStackTrace((Throwable) object);
    } else if (object.getClass().isEnum()) {
      return object.toString();
    } else if (object.getClass().isArray()) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(object.getClass().getSimpleName());
      int length = Array.getLength(object);
      stringBuilder.insert(stringBuilder.length() - 1, Array.getLength(object)).append("{");
      for (int i = 0; i < length; i++) {
        stringBuilder.append(deepToString(Array.get(object, i)));
        if (i < length - 1) stringBuilder.append(',');
      }
      return stringBuilder.append("}").toString();
    } else if (List.class.isAssignableFrom(object.getClass())) {
      List<?> listObject = ((List<?>) object);
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(object.getClass().getSimpleName()).append('[').append(listObject.size()).append(']');
      stringBuilder.append("{");
      for (Object subObject : listObject) {
        stringBuilder.append(deepToString(subObject)).append(',');
      }
      if (!listObject.isEmpty()) stringBuilder.deleteCharAt(stringBuilder.length() - 1);
      return stringBuilder.append('}').toString();
    } else {
      try {
        Method toStringMethod = Invocation.getPublicMethod(object.getClass(), "toString");
        if (toStringMethod.getDeclaringClass() == Object.class) return toStringByFields(object, false);
      } catch (Throwable t) {
      }
      try {
        return object.toString();
      } catch (Throwable t) {
        return "FailToString";
      }
    }
  }

  public static boolean deepEquals(Object object0, Object object1) {
    if (object0 == null && object1 == null) return true;
    if (object0 == null && object1 != null) return false;
    if (object0 != null && object1 == null) return false;
    if (!object0.getClass().equals(object1.getClass())) return false;
    if (object0.equals(object1)) return true;
    if (object0.getClass().isArray() && Array.getLength(object0) == Array.getLength(object1)) {
      int length = Array.getLength(object0);
      for (int i = 0; i < length; ++i) {
        if (!deepEquals(Array.get(object0, i), Array.get(object1, i))) return false;
      }
      return true;
    }
    return false;
  }

  public static String getString(char c, int number) {
    StringBuilder stringBuilder = new StringBuilder(number);
    for (int i = 0; i < number; ++i) {
      stringBuilder.append(c);
    }
    return stringBuilder.toString();
  }

  public static String getEnvVariable(String name) throws IOException {
    BufferedReader stdOutputReader = null;
    String output = "";
    try {
      Process process = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "echo $" + name });
      stdOutputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = stdOutputReader.readLine()) != null) {
        output += line;
      }
    } catch (Throwable t) {
    } finally {
      if (stdOutputReader != null) stdOutputReader.close();
    }

    return output;
  }

  public static String lsInRemote(String host, String path) throws IOException {
    return runCommandInRemoteHost(host, "ls -la " + path, 0);
  }

  public static void mkdirsInRemote(String host, String path, boolean deleteIfExisted) throws IOException {
    if (deleteIfExisted) deleteInRemote(host, path);
    if (!runCommandInRemoteHost(host, "mkdir -p " + path, 0).isEmpty())
      throw new IOException("fail to delete " + host + ":" + path);
  }

  public static void deleteInRemote(String host, String path) throws IOException {
    if (!runCommandInRemoteHost(host, "rm -rf " + path, 0).isEmpty())
      throw new IOException("fail to delete " + path + " in " + host);
  }

  public static String runCommandInRemoteHost(String host, String command, Integer expectedExitCode) throws IOException {
    command = command.replaceAll("\"", "\\\\\"");
    command = command.replaceAll("\\$", "\\\\\\$");
    return runCommand("ssh " + host + " \"" + command + "\"", expectedExitCode, null, null);
  }

  public static Process runCommand(String command, String extraPath, String libPath) throws IOException {
    return runCommand(new String[] { "/bin/bash", "-c", command }, extraPath, libPath);
  }

  public static Process runCommand(String[] cmdArray, String extraPath, String libPath) throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder(cmdArray);
    Map<String, String> environment = processBuilder.environment();
    if (extraPath != null) {
      if (extraPath.isEmpty()) extraPath = ".";
      environment.put("PATH", extraPath + ":" + environment.get("PATH"));
    }
    if (libPath != null) {
      if (libPath.isEmpty()) libPath = ".";
      environment.put("LD_LIBRARY_PATH", libPath + ":" + environment.get("LD_LIBRARY_PATH"));
    }
    return processBuilder.start();
  }

  public static void mkdirs(String path, boolean deleteBeforeCreate) throws IOException {
    File file = new File(path);
    if (deleteBeforeCreate) delete(file);
    file.mkdirs();
  }

  public static void delete(File file) throws IOException {
    try {
      if (file.isDirectory()) {
        for (File subFile : file.listFiles()) {
          delete(subFile);
        }
      }
      if (file.exists()) file.delete();
    } catch (IOException e) {
      throw e;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static String runCommand(String command, Integer expectedExitCode, String extraPath, String libPath)
      throws IOException {
    return runCommand(command, expectedExitCode, extraPath, libPath, true);
  }

  public static String runCommand(String command, Integer expectedExitCode, String extraPath, String libPath,
      boolean destory) throws IOException {
    BufferedReader stdOutputReader = null;
    StringBuilder output = new StringBuilder();
    Process process = runCommand(command, extraPath, libPath);;
    try {
      stdOutputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      while ((line = stdOutputReader.readLine()) != null) {
        output.append(line).append('\n');
      }
      if (output.length() > 0) output.deleteCharAt(output.length() - 1);
      if (expectedExitCode != null && process.waitFor() != expectedExitCode)
        throw new IOException("exit code=" + process.exitValue() + ", expected exit code=" + expectedExitCode);
    } catch (Throwable t) {
      destory = true;
      throw new IOException("fail to exceute " + command + ", output=" + output
          + (extraPath == null ? "" : ", extraPath=" + extraPath) + (libPath == null ? "" : ", libPath=" + libPath), t);
    } finally {
      if (stdOutputReader != null) stdOutputReader.close();
      process.getOutputStream().close();
      process.getInputStream().close();
      process.getErrorStream().close();
      if (destory) process.destroy();
    }
    return output.toString();
  }

  public static String setConfDefaultValue(Configuration conf, String key, Object value) {
    return setConfDefaultValue(conf, key, value, true, false);
  }

  public static String setConfDefaultValue(Configuration conf, String key, Object value, boolean setIfOnlyWhitespace) {
    return setConfDefaultValue(conf, key, value, setIfOnlyWhitespace, false);
  }

  public static String setConfDefaultValue(Configuration conf, String key, Object value, boolean setIfOnlyWhitespace,
      boolean relativePathToAbosulotePath) {
    String valueString = conf.getRaw(key);
    if (value != null && (valueString == null || (setIfOnlyWhitespace && valueString.trim().isEmpty())))
      valueString = value.toString();
    if (relativePathToAbosulotePath) valueString = new File(valueString).getAbsolutePath();
    conf.set(key, valueString);
    return conf.getRaw(key);
  }

  public static String getPath(String parentPath, String subPath) {
    if (parentPath == null) parentPath = ".";
    if (parentPath.isEmpty()) parentPath = ".";
    if (subPath == null) subPath = ".";
    if (subPath.isEmpty()) subPath = ".";
    return new StringBuilder(256).append(parentPath).append('/').append(subPath).toString();
  }

  public static String getHost(String addressWithHostAndPort) {
    if (addressWithHostAndPort == null) return null;
    String[] addressParts = addressWithHostAndPort.split(":", 2);
    if (addressParts.length < 2) return addressWithHostAndPort;
    return addressParts[0];
  }

  public static Integer getPort(String addressWithHostAndPort) {
    if (addressWithHostAndPort == null) return null;
    try {
      return Integer.valueOf(addressWithHostAndPort.split(":", 2)[1]);
    } catch (Throwable t) {
      logWarn(logger, "fail to get port from ", addressWithHostAndPort, t);
      return null;
    }
  }

  public static boolean isLocalHost(String host) {
    return host.startsWith("127") || host.endsWith("localhost");
  }

  public static String getOsType() throws IOException {
    String osBits = runCommand("uname -a", null, null, null).contains("x86_64") ? "64" : "32";
    String osName = runCommand("cat /proc/version", null, null, null).toLowerCase();
    if (osName.contains("ubuntu")) return osName = "ubuntu" + osBits;
    else if (osName.contains("red hat")) return osName = "redhat" + osBits;
    else return "";
  }

  public static String getCurrentUser() {
    return System.getProperty("user.name");
  }

  public static Comparator<InetAddress> inetAddressComparator = new Comparator<InetAddress>() {
    @Override
    public int compare(InetAddress address1, InetAddress address2) {
      try {
        Field intAddressField = InetAddress.class.getDeclaredField("address");
        intAddressField.setAccessible(true);
        Long longOfAddress1 = 0x00000000FFFFFFFFL & intAddressField.getInt(address1);
        Long longOfAddress2 = 0x00000000FFFFFFFFL & intAddressField.getInt(address2);
        return longOfAddress1.compareTo(longOfAddress2);
      } catch (Throwable t) {
        if (logger.isWarnEnabled()) logger.warn("fail to compare ip address", t);
        return -1;
      }
    }
  };

  public static List<InetAddress> getWanInetAddressList(List<InetAddress> addressList) throws IOException {
    return getInetAddressList(addressList, "wan");
  }

  public static List<InetAddress> getLanInetAddressList(List<InetAddress> addressList) throws IOException {
    return getInetAddressList(addressList, "lan");
  }

  public static List<InetAddress> getLoopbackInetAddressList(List<InetAddress> addressList) throws IOException {
    return getInetAddressList(addressList, "loopback");
  }

  public static List<InetAddress> getLinklocalInetAddressList(List<InetAddress> addressList) throws IOException {
    return getInetAddressList(addressList, "linklocal");
  }

  public static List<InetAddress> getInetAddressList(List<InetAddress> addressList, String type) throws IOException {
    type = type.toLowerCase();
    List<InetAddress> newAddressList = new ArrayList<InetAddress>();
    for (InetAddress address : addressList) {
      if (address.isLinkLocalAddress()) {
        if (type.equals("linklocal")) newAddressList.add(address);
      } else if (address.isLoopbackAddress()) {
        if (type.equals("loopback")) newAddressList.add(address);
      } else if (address.isSiteLocalAddress()) {
        if (type.equals("lan")) newAddressList.add(address);
      } else {
        if (type.equals("wan")) newAddressList.add(address);
      }
    }
    Collections.sort(newAddressList, inetAddressComparator);
    return newAddressList;
  }

  public static List<InetAddress> getInetAddressList() throws IOException {
    List<InetAddress> addressList = new ArrayList<InetAddress>();
    Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
    while (netInterfaces.hasMoreElements()) {
      NetworkInterface netInterface = netInterfaces.nextElement();
      Enumeration<InetAddress> inetAddresses = netInterface.getInetAddresses();
      while (inetAddresses.hasMoreElements()) {
        InetAddress inetAddress = inetAddresses.nextElement();
        if (inetAddress != null && inetAddress instanceof Inet4Address) addressList.add(inetAddress);
      }
    }

    List<InetAddress> newAddressList = getWanInetAddressList(addressList);
    newAddressList.addAll(getLanInetAddressList(addressList));
    newAddressList.addAll(getLoopbackInetAddressList(addressList));
    newAddressList.addAll(getLinklocalInetAddressList(addressList));

    return newAddressList;
  }

  public static String logTrace(Logger logger, Object... objects) {
    return log(logger, Level.TRACE, objects);
  }

  public static String logDebug(Logger logger, Object... objects) {
    return log(logger, Level.DEBUG, objects);
  }

  public static String logInfo(Logger logger, Object... objects) {
    return log(logger, Level.INFO, objects);
  }

  public static String logWarn(Logger logger, Object... objects) {
    return log(logger, Level.WARN, objects);
  }

  public static String logError(Logger logger, Object... objects) {
    return log(logger, Level.ERROR, objects);
  }

  public static String log(Logger logger, Level level, Object... objects) {
    if (logger == null || objects == null || objects.length == 0) return "";
    if (level.toInt() == Level.TRACE_INT && !logger.isTraceEnabled()) return "";
    if (level.toInt() == Level.DEBUG_INT && !logger.isDebugEnabled()) return "";
    if (level.toInt() == Level.INFO_INT && !logger.isInfoEnabled()) return "";
    if (level.toInt() == Level.WARN_INT && !logger.isWarnEnabled()) return "";
    if (level.toInt() == Level.ERROR_INT && !logger.isErrorEnabled()) return "";
    return log(logger, level, objects, null);
  }

  public static String log(Logger logger, Level level, Invocation objecsFromInvocation, Object... objects) {
    if (logger == null || (objecsFromInvocation == null && (objects == null || objects.length == 0))) return "";
    if (level.toInt() == Level.TRACE_INT && !logger.isTraceEnabled()) return "";
    if (level.toInt() == Level.DEBUG_INT && !logger.isDebugEnabled()) return "";
    if (level.toInt() == Level.INFO_INT && !logger.isInfoEnabled()) return "";
    if (level.toInt() == Level.WARN_INT && !logger.isWarnEnabled()) return "";
    if (level.toInt() == Level.ERROR_INT && !logger.isErrorEnabled()) return "";
    try {
      Object result = objecsFromInvocation == null ? null : objecsFromInvocation.invoke();
      Object[] resultObjectArray = result instanceof Object[] ? (Object[]) result : null;
      return log(logger, level, resultObjectArray, objects);
    } catch (IOException e) {
      Utilities.logWarn(logger, "fail to log for ", objecsFromInvocation, e);
      return "";
    }
  }

  public static String log(Logger logger, Level level, Object[] objects0, Object[] objects1) {
    if (logger == null || ((objects0 == null || objects0.length == 0) && (objects1 == null || objects1.length == 0)))
      return "";
    if (level.toInt() == Level.TRACE_INT && !logger.isTraceEnabled()) return "";
    if (level.toInt() == Level.DEBUG_INT && !logger.isDebugEnabled()) return "";
    if (level.toInt() == Level.INFO_INT && !logger.isInfoEnabled()) return "";
    if (level.toInt() == Level.WARN_INT && !logger.isWarnEnabled()) return "";
    if (level.toInt() == Level.ERROR_INT && !logger.isErrorEnabled()) return "";
    StringBuilder messageBuilder = new StringBuilder(256);
    Object lastObject = null;
    if (objects1 == null || objects1.length == 0) {
      for (int i = 0; i < objects0.length - 1; ++i) {
        messageBuilder.append(deepToString(objects0[i]));
      }
      lastObject = objects0[objects0.length - 1];
    } else {
      if (objects0 != null && objects0.length > 0) {
        for (int i = 0; i < objects0.length; ++i) {
          messageBuilder.append(deepToString(objects0[i]));
        }
      }
      for (int i = 0; i < objects1.length - 1; ++i) {
        messageBuilder.append(deepToString(objects1[i]));
      }
      lastObject = objects1[objects1.length - 1];
    }
    if (lastObject instanceof Throwable) {
      if (level.toInt() == Level.TRACE_INT) logger.trace(messageBuilder.toString(), (Throwable) lastObject);
      if (level.toInt() == Level.DEBUG_INT) logger.debug(messageBuilder.toString(), (Throwable) lastObject);
      if (level.toInt() == Level.INFO_INT) logger.info(messageBuilder.toString(), (Throwable) lastObject);
      if (level.toInt() == Level.WARN_INT) logger.warn(messageBuilder.toString(), (Throwable) lastObject);
      if (level.toInt() == Level.ERROR_INT) logger.error(messageBuilder.toString(), (Throwable) lastObject);
      messageBuilder.append(getThrowableStackTrace((Throwable) lastObject));
    } else {
      messageBuilder.append(deepToString(lastObject));
      if (level.toInt() == Level.TRACE_INT) logger.trace(messageBuilder.toString());
      if (level.toInt() == Level.DEBUG_INT) logger.debug(messageBuilder.toString());
      if (level.toInt() == Level.INFO_INT) logger.info(messageBuilder.toString());
      if (level.toInt() == Level.WARN_INT) logger.warn(messageBuilder.toString());
      if (level.toInt() == Level.ERROR_INT) logger.error(messageBuilder.toString());
    }
    return messageBuilder.append('\n').toString();
  }

  public static String getThrowableStackTrace(Throwable t) {
    PrintStream printStream = null;
    try {
      OutputStream outputStream = null;
      outputStream = new ByteArrayOutputStream(1024);
      printStream = new PrintStream(outputStream);
      t.printStackTrace(printStream);
      printStream.flush();
      return outputStream.toString();
    } catch (Exception e) {
      return "unknow";
    } finally {
      if (printStream != null) printStream.close();
    }
  }

  public static void configureLog4j(Configuration conf, String loggerConfKeyPrefix, Level level) throws IOException {
    if (conf == null) conf = new Configuration(false);
    if (loggerConfKeyPrefix == null) loggerConfKeyPrefix = "";
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.rootLogger", "INFO,console");
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.threshhold", "ALL");
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console", "org.apache.log4j.ConsoleAppender");
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.target", "System.err");
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
    setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.layout.ConversionPattern",
        "%d{yy-MM-dd HH:mm:ss,SSS} %p %c{2}: %m%n");
    Properties props = new Properties();
    Map<String, String> confKeyAndValues = getConf(conf, loggerConfKeyPrefix);
    for (String key : confKeyAndValues.keySet()) {
      props.setProperty(key.substring(loggerConfKeyPrefix.length()), confKeyAndValues.get(key));
    }
    new PropertyConfigurator().doConfigure(props, LogManager.getLoggerRepository());
    for (String key : confKeyAndValues.keySet()) {
      Utilities.log(logger, level, "set ", key.substring(loggerConfKeyPrefix.length()), "=", confKeyAndValues.get(key));
    }
  }

  public static List<String> getListenAddressList(String[]... features) throws IOException {
    String[] includes = features.length > 0 ? features[0] : null;
    String[] excludes = features.length > 1 ? features[1] : null;
    String[] fields = features.length > 2 ? features[2] : new String[] { "4" };
    String commandForGetListeningAddress = "netstat -lntp | grep tcp" + "|awk '{print ";
    for (String field : fields) {
      commandForGetListeningAddress += "$" + field + "\",\" ";
    }
    commandForGetListeningAddress += "}'";
    String addresses = Utilities.runCommand(commandForGetListeningAddress, null, null, null);
    List<String> addressList = new ArrayList<String>();
    for (String address : addresses.split("\n")) {
      address = StringUtils.deleteWhitespace(address);
      if (stringFilter(address, includes, excludes)) addressList.add(address);
    }
    return addressList;
  }

  public static boolean stringFilter(String string, String[] includes, String[] excludes) {
    if (includes != null) {
      for (String include : includes) {
        if (!string.contains(include)) { return false; }
      }
    }
    if (excludes != null) {
      for (String exclude : excludes) {
        if (string.contains(exclude)) { return false; }
      }
    }
    return true;
  }

  public static void killProcesses(String pids) throws IOException {
    pids = StringUtils.deleteWhitespace(pids).replaceAll(" ", "");
    String[] pidArray = pids.replace("\n", ",").split(",");
    for (String pid : pidArray) {
      killProcess(pid);
    }
  }

  public static void killProcess(String pid) throws IOException {
    for (int i = 0; i < 10; ++i) {
      try {
        runCommand("kill " + pid, 0, null, null);
        return;
      } catch (IOException e) {
        if (runCommand("ps -ef|awk '{print $2}'|grep " + pid, null, null, null).isEmpty()) return;
        if (i == 9) throw e;
        sleepAndProcessInterruptedException(1000, logger);
      }
    }
  }

  public static long elapsedTime(long startTime) {
    return System.currentTimeMillis() - startTime;
  }

  public static boolean waitValue(Object object, Object expectedValue, Long timeout) {
    try {
      return waitValue(object, expectedValue, timeout, true);
    } catch (InterruptedException e) {
      throw new RuntimeException("never to here");
    }
  }

  public static boolean waitValue(Object object, Object expectedValue, Long timeout, boolean ignoreInterruptedException)
      throws InterruptedException {
    long startTime = System.currentTimeMillis();
    Object innerValue = null;
    while (true) {
      if (object instanceof AtomicBoolean) innerValue = ((AtomicBoolean) object).get();
      else if (object instanceof AtomicInteger) innerValue = ((AtomicInteger) object).get();
      else if (object instanceof AtomicLong) innerValue = ((AtomicLong) object).get();
      else innerValue = object;
      if (innerValue.equals(expectedValue)) break;
      if (timeout != null && System.currentTimeMillis() - startTime > timeout) return false;
      if (ignoreInterruptedException) sleepAndProcessInterruptedException(1, logger);
      else Thread.sleep(1);
    }
    return true;
  }

  public static Object repeatInvoke(long repeat, Invocation invocation) throws IOException {
    for (int i = 0; i < repeat; ++i) {
      invocation.invoke();
    }
    return invocation.getResult();
  }

  public static Object retryInvoke(int retryTimes, long retryDelayTime, Logger logger, Invocation callback,
      Object object, Class<?> delcaredClass, String methodName, Class<?>[] parameterClasses, Object... parameters)
      throws Throwable {
    Invocation invocation = new Invocation(delcaredClass, methodName, parameterClasses, parameters);
    return retryInvoke(retryTimes, retryDelayTime, logger, callback, invocation);
  }

  public static Object retryInvoke(int retryTimes, long retryDelayTime, Logger logger, Invocation callback,
      Invocation invocation) throws Throwable {
    Throwable throwable = null;
    if (logger == null) logger = Utilities.logger;
    for (int i = 0; i < retryTimes; ++i) {
      try {
        return invocation.invoke();
      } catch (Throwable t) {
        if (callback != null) callback.invoke();
        Utilities.logWarn(logger, " fail to call ", invocation, " retryIndex=", i);
        sleepAndProcessInterruptedException(retryDelayTime, logger);
        throwable = t;
      }
    }
    throw throwable;
  }

  /**
   * not support more override method, ASM is a better solution.
   * refer: http://stackoverflow.com/questions/4024587/get-callers-method-not-name
   */
  public static String getCallerName() throws IOException {
    return Thread.currentThread().getStackTrace()[3].getMethodName();
  }

  /**
   * not support more override method, ASM is a better solution.
   * refer: http://stackoverflow.com/questions/4024587/get-callers-method-not-name
   */
  public static String getMethodName() throws IOException {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }

  /**
   * not support more override method, ASM is a better solution.
   * refer: http://stackoverflow.com/questions/4024587/get-callers-method-not-name
   */
  public static Method getCaller(boolean staticMethod) throws IOException {
    StackTraceElement caller = Thread.currentThread().getStackTrace()[3];
    Class<?> clazz = ClassCache.getWithIOException(caller.getClassName());
    for (Method method : clazz.getDeclaredMethods()) {
      if (!method.getName().equals(caller.getMethodName())) continue;
      if (Modifier.isStatic(method.getModifiers()) != staticMethod) continue;
      return method;
    }
    throw new IOException("fail to get caller");
  }

  public static Throwable getFirstCause(Throwable t) {
    if (t == null) return null;
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }

  public static int getPid() {
    return Integer.parseInt(getPidString());
  }

  static String pidString = null;

  public static String getPidString() {
    if (pidString != null) return pidString;
    String name = ManagementFactory.getRuntimeMXBean().getName();
    return pidString = name.substring(0, name.indexOf('@'));
  }

  static Map<Long, String> threadDescriptions = new HashMap<Long, String>();

  public static String getCurrentThreadDescription() {
    long threadId = Thread.currentThread().getId();
    String threadDescription = threadDescriptions.get(threadId);
    if (threadDescription != null) return threadDescription;
    StringBuilder threadDescriptionBuilder = new StringBuilder(100);
    threadDescriptionBuilder.append("processId=").append(Utilities.getPidString()).append("|");
    threadDescriptionBuilder.append("threadId=").append(Thread.currentThread().getId()).append("|");
    threadDescriptionBuilder.append("threadName=").append(Thread.currentThread().getName());
    threadDescriptions.put(threadId, threadDescription = threadDescriptionBuilder.toString());
    return threadDescription;
  }

  public static File getLibPath() {
    Class<?> clazz = Utilities.class;
    String pathOfClassInPackage = "/" + clazz.getName().replaceAll("\\.", "/") + ".class";
    URL urlOfClass = clazz.getResource("/" + clazz.getName().replaceAll("\\.", "/") + ".class");
    if (urlOfClass == null) return null;
    String classOfPath = urlOfClass.getPath().substring(0, urlOfClass.getPath().lastIndexOf(pathOfClassInPackage));
    if (urlOfClass.getProtocol().equals("jar"))
      classOfPath = new File(classOfPath.substring(5, classOfPath.length() - 1)).getParent();
    return new File(classOfPath);
  }

  public static String getClassPath(Class<?> clazz) {
    return clazz.getResource("/" + clazz.getName().replaceAll("\\.", "/") + ".class").getPath();
  }

  public static void greaterAndSet(long value, AtomicLong atomicValue) {
    synchronized (atomicValue) {
      if (value > atomicValue.get()) atomicValue.set(value);
    }
  }

  public static String normalizePath(String path) throws IOException {
    if (path == null) throw new IOException("path is null");
    if (!path.startsWith("/")) throw new IOException("path is not start with /");
    while (path.contains("//")) {
      path.replaceAll("//", "/");
    }
    if (path.endsWith("/") && path.length() > 1) path = path.substring(0, path.length() - 1);
    return path;
  }

  public static String[] getNamesInPath(String path) throws IOException {
    path = normalizePath(path);
    return path.length() == 1 ? new String[] { "" } : path.split("/");
  }

  public static String getPathInName(String[] names, int index) throws IOException {
    if (index >= names.length) index = names.length - 1;
    if (index < 0) index = 0;
    if (names.length == 0) throw new IOException("names.length is 0");
    if (index == 0) return "/";
    StringBuilder path = new StringBuilder(1024);
    for (int i = 1; i <= index; ++i) {
      if (names[i] == null) throw new IOException("names[" + i + "] is null, names=" + Arrays.deepToString(names));
      path.append("/").append(names[i]);
    }
    return path.toString();
  }

  public static String getSqlTypeForJavaType(Class<?> type) throws IOException {
    if (type == null) throw new IOException("type is null");
    if (type.equals(Byte.TYPE) || type.equals(Byte.class)) return "TINYINT";
    if (type.equals(Short.TYPE) || type.equals(Short.class)) return "SMALLINT";
    if (type.equals(Integer.TYPE) || type.equals(Integer.class)) return "INT";
    if (type.equals(Long.TYPE) || type.equals(Long.class)) return "BIGINT";
    if (type.equals(Float.TYPE) || type.equals(Float.class)) return "FLOAT";
    if (type.equals(Double.TYPE) || type.equals(Double.class)) return "DOUBLE";
    if (type.equals(String.class)) return "VARCHAR";
    else throw new IOException("unsupportted type=" + type.getSimpleName());
  }

  public static Object getFieldValue(Object object, String fieldName) throws IOException {
    try {
      return getField(object.getClass(), fieldName).get(object);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static void setFieldValue(Object object, String fieldName, Object fieldValue) throws IOException {
    try {
      if (object instanceof Class) getField((Class<?>) object, fieldName).set(null, fieldValue);
      else getField(object.getClass(), fieldName).set(object, fieldValue);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public static Field getField(Class<?> clazz, String fieldName) throws IOException {
    if (clazz == null) throw new IOException("clazz is null");
    if (fieldName == null) throw new IOException("fieldName is null");
    if (fieldName.isEmpty()) throw new IOException("fieldName is empty");
    Field field = null;
    try {
      field = clazz.getDeclaredField(fieldName);
    } catch (Throwable t0) {
      try {
        field = clazz.getField(fieldName);
      } catch (Throwable t1) {
        if (clazz.getSuperclass() != null) {
          field = getField(clazz.getSuperclass(), fieldName);
        } else throw new IOException("fail to get field for " + clazz.getName() + "." + fieldName, t1);
      }
    }
    field.setAccessible(true);
    return field;
  }

  public static List<Field> getFields(Class<?> clazz) throws IOException {
    if (clazz == null) throw new IOException("clazz is null");
    List<Field> fields = new ArrayList<Field>();
    try {
      for (Field field : clazz.getDeclaredFields()) {
        field.setAccessible(true);
        fields.add(field);
      }
      if (clazz.getSuperclass() != null) {
        fields.addAll(getFields(clazz.getSuperclass()));
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
    return fields;
  }

  public static <T> List<T> addArrayToList(T[] array, List<T> list) {
    if (list == null || array == null) return null;
    for (T object : array) {
      list.add(object);
    }
    return list;
  }
}
