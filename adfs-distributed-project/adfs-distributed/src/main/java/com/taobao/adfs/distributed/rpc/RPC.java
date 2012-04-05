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

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;

import com.taobao.adfs.util.Utilities;

/**
 * A simple RPC mechanism.
 * 
 * A <i>protocol</i> is a Java interface. All parameters and return types must be one of:
 * 
 * <ul>
 * <li>a primitive type, <code>boolean</code>, <code>byte</code>, <code>char</code>, <code>short</code>,
 * <code>int</code>, <code>long</code>, <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 * 
 * <li>a {@link String}; or</li>
 * 
 * <li>a {@link Writable}; or</li>
 * 
 * <li>an array of the above types</li>
 * </ul>
 * 
 * All methods in the protocol should throw only IOException. No field data of the protocol instance is transmitted.
 */
public class RPC {
  private static final Log LOG = LogFactory.getLog(RPC.class);

  private RPC() {
  } // no public ctor

  /** A method invocation, including the method name and its parameters. */
  public static class Invocation implements Writable {
    private Class<?> declaredClass;
    private String methodName;
    private Class<?>[] parameterClasses;
    private Object[] parameters;
    private Method method = null;
    private Object result = null;
    private byte[] byteArray = null;
    private boolean haveInvoked = false;
    private Object object = null;
    private Invocation proxyInvocation = null;
    private long elapsedTime = -1;
    protected String callerOfCallerAddress = null;
    protected String callerAddress = null;
    protected int callerProcessId = 0;
    protected long callerThreadId = 0;
    protected long callerSequenceNumber = -1;
    protected String callerThreadName = null;

    public Invocation() {
    }

    public Invocation(String className, String methodName, Object... parameters) throws IOException {
      try {
        this.declaredClass = ClassCache.get(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      this.methodName = methodName;
      this.parameters = parameters == null ? new Object[] {} : parameters;
      parameterClasses = new Class<?>[this.parameters.length];
      for (int i = 0; i < this.parameters.length; ++i) {
        parameterClasses[i] = this.parameters[i].getClass();
      }
      method = getMethod(declaredClass, methodName, parameterClasses);
    }

    public Invocation(Object object, String methodName, Object... parameters) throws IOException {
      if (object == null) throw new IOException("object is null");
      this.object = object;
      this.declaredClass = object.getClass();
      this.methodName = methodName;
      this.parameters = parameters == null ? new Object[] {} : parameters;
      parameterClasses = new Class<?>[this.parameters.length];
      for (int i = 0; i < this.parameters.length; ++i) {
        parameterClasses[i] = this.parameters[i].getClass();
      }
      method = getMethod(declaredClass, methodName, parameterClasses);
    }

    public Invocation(Object object, String methodName, Class<?>[] parameterClasses, Object... parameters)
        throws IOException {
      if (object == null) throw new IOException("object is null");
      this.object = object;
      this.declaredClass = object.getClass();
      this.methodName = methodName;
      this.parameterClasses = parameterClasses;
      method = getMethod(declaredClass, methodName, parameterClasses);
      this.parameters = new Object[parameterClasses.length];
      if (parameters == null) return;
      for (int i = 0; i < parameters.length; ++i) {
        this.parameters[i] = parameters[i];
      }
    }

    public Invocation(Class<?> clazz, String methodName, Class<?>[] parameterClasses, Object... parameters)
        throws IOException {
      this.declaredClass = clazz;
      this.methodName = methodName;
      this.parameterClasses = parameterClasses;
      method = getMethod(declaredClass, methodName, parameterClasses);
      this.parameters = new Object[parameterClasses.length];
      if (parameters == null) return;
      for (int i = 0; i < parameters.length; ++i) {
        this.parameters[i] = parameters[i];
      }
    }

    public Invocation(Class<?> clazz, String methodName, Object... parameters) throws IOException {
      this.declaredClass = clazz;
      this.methodName = methodName;
      this.parameters = parameters == null ? new Object[] {} : parameters;
      parameterClasses = new Class<?>[this.parameters.length];
      for (int i = 0; i < this.parameters.length; ++i) {
        parameterClasses[i] = this.parameters[i].getClass();
      }
      method = getMethod(declaredClass, methodName, parameterClasses);
    }

    public Invocation(Method method, Object... parameters) {
      this.method = method;
      this.declaredClass = method.getDeclaringClass();
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      if (parameters == null) this.parameters = new Object[] {};
    }

    public Invocation(Object object, Method method, Object... parameters) {
      this.object = object;
      this.method = method;
      this.declaredClass = method.getDeclaringClass();
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
      if (parameters == null) this.parameters = new Object[] {};
    }

    public byte[] toByteArray() throws IOException {
      if (byteArray == null) {
        // no need to flush/close DataOutputBuffer
        DataOutputBuffer dataOutputBuffer = new DataOutputBuffer();
        write(dataOutputBuffer);
        byteArray = Arrays.copyOf(dataOutputBuffer.getData(), dataOutputBuffer.getLength());
      }
      return byteArray;
    }

    public Invocation(byte[] byteArray) throws IOException {
      this.byteArray = Arrays.copyOf(byteArray, byteArray.length);
      // no need to close DataOutputBuffer
      DataInputBuffer dataInputBuffer = new DataInputBuffer();
      dataInputBuffer.reset(this.byteArray, this.byteArray.length);
      readFields(dataInputBuffer);
    }

    static public Method getDeclaredMethod(Class<?> clazz, String methodName, Class<?>... parameterClasses)
        throws IOException {
      try {
        return clazz.getDeclaredMethod(methodName, parameterClasses);
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }

    static public Method getPublicMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes)
        throws IOException {
      try {
        return clazz.getMethod(methodName, parameterTypes);
      } catch (Throwable t) {
        throw new IOException(t);
      }
    }

    static public Method getMethod(Object object, String methodName, Class<?>... parameterClasses) throws IOException {
      return getMethod(object.getClass(), methodName, parameterClasses);
    }

    static public Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterClasses) throws IOException {
      try {
        Method method = getDeclaredMethod(clazz, methodName, parameterClasses);
        method.setAccessible(true);
        return method;
      } catch (Throwable t) {
        return getPublicMethod(clazz, methodName, parameterClasses);
      }
    }

    public Method getMethod() throws IOException {
      if (method == null) method = getMethod(declaredClass, methodName, parameterClasses);
      return method;
    }

    public Object invoke() throws IOException {
      if (object == null) throw new IOException("object is null");
      return invoke(object);
    }

    public Object invoke(Object object) throws IOException {
      long startTime = System.currentTimeMillis();
      try {
        result = getMethod().invoke(object, parameters);
        haveInvoked = true;
        byteArray = null;
        return result;
      } catch (Throwable t) {
        throw new IOException(t);
      } finally {
        elapsedTime = System.currentTimeMillis() - startTime;
      }
    }

    /** The name of the method invoked. */
    public String getMethodName() {
      return methodName;
    }

    /** The parameter classes. */
    public Class<?>[] getParameterClasses() {
      return parameterClasses;
    }

    /** The parameter instances. */
    public Object[] getParameters() {
      return parameters;
    }

    public void resetResult() {
      result = null;
      haveInvoked = false;
      byteArray = null;
      elapsedTime = -1;
    }

    public Object setResult(Object result) {
      this.result = result;
      haveInvoked = true;
      byteArray = null;// so to byte array can regenerate it
      return result;
    }

    public Object getResult() {
      return result;
    }

    public void setCallerOfCallerAddress(String callerOfCallerAddress) {
      callerName = null;
      this.callerOfCallerAddress = callerOfCallerAddress;
    }

    public void setCallerAddress(String callerAddress) {
      this.callerAddress = callerAddress;
      callerName = null;
      simpleCallerName = null;
      operateIdentifier = null;
    }

    public void setCallerProcessId(int callerProcessId) {
      callerName = null;
      simpleCallerName = null;
      operateIdentifier = null;
      this.callerProcessId = callerProcessId;
    }

    public void setCallerThreadId(long callerThreadId) {
      callerName = null;
      simpleCallerName = null;
      operateIdentifier = null;
      this.callerThreadId = callerThreadId;
    }

    public void setCallerThreadName(String callerThreadName) {
      callerName = null;
      simpleCallerName = null;
      this.callerThreadName = callerThreadName;
    }

    public void setCallerSequenceNumber(long callerSequenceNumber) {
      callerName = null;
      operateIdentifier = null;
      this.callerSequenceNumber = callerSequenceNumber;
    }

    public String getCallerAddress() {
      return callerAddress;
    }

    public String getCallerOfCallerAddress() {
      return callerOfCallerAddress;
    }

    public int getCallerProcessId() {
      return callerProcessId;
    }

    public long getCallerThreadId() {
      return callerThreadId;
    }

    public long getCallerSequenceNumber() {
      return callerSequenceNumber;
    }

    public String getCallerThreadName() {
      return callerThreadName;
    }

    private String operateIdentifier = null;

    public String getOperateIdentifier() {
      if (callerProcessId == 0) return null;
      if (operateIdentifier != null) return operateIdentifier;
      return callerAddress + "-" + callerProcessId + "-" + callerThreadId + "-" + callerSequenceNumber;
    }

    private String callerName = null;
    private String simpleCallerName = null;

    public String getCallerName(String prefix, String postfix, boolean simple) {
      if (callerProcessId == 0) return null;
      if (!simple && callerName != null) return callerName;
      if (simple && simpleCallerName != null) return simpleCallerName;
      StringBuilder stringBuilder = new StringBuilder(1024);
      if (prefix != null) stringBuilder.append(prefix);
      stringBuilder.append("address=").append(callerAddress);
      stringBuilder.append("|").append("processId=").append(callerProcessId);
      stringBuilder.append("|").append("threadId=").append(callerThreadId);
      stringBuilder.append("|").append("threadName=").append(callerThreadName);
      if (!simple) stringBuilder.append("|").append("sequenceNumber=").append(callerSequenceNumber);
      if (!simple) stringBuilder.append("|").append("callerOfCallerAddress=").append(callerOfCallerAddress);
      if (postfix != null) stringBuilder.append(postfix);
      return callerName = stringBuilder.toString();
    }

    public long getElapsedTime() {
      return elapsedTime;
    }

    public long setElapsedTime(long elapsedTime) {
      return this.elapsedTime = elapsedTime;
    }

    public Invocation getProxyInvocation() {
      return proxyInvocation;
    }

    public Invocation setProxyInvocation(Invocation proxyInvocation) {
      return this.proxyInvocation = proxyInvocation;
    }

    public void setObject(Object object) {
      this.object = object;
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(callerProcessId);
      out.writeLong(callerThreadId);
      out.writeLong(callerSequenceNumber);
      ObjectWritable.writeString(out, callerThreadName);
      ObjectWritable.writeString(out, callerAddress);
      ObjectWritable.writeString(out, callerOfCallerAddress);
      ObjectWritable.writeString(out, declaredClass.getName());
      ObjectWritable.writeString(out, methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i]);
      }
      ObjectWritable.writeObject(out, result, Object.class);
    }

    public void readFields(DataInput in) throws IOException {
      callerProcessId = in.readInt();
      callerThreadId = in.readLong();
      callerSequenceNumber = in.readLong();
      callerThreadName = ObjectWritable.readString(in);
      callerAddress = ObjectWritable.readString(in);
      callerOfCallerAddress = ObjectWritable.readString(in);
      try {
        declaredClass = ClassCache.get(ObjectWritable.readString(in));
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      methodName = ObjectWritable.readString(in);
      parameterClasses = new Class[in.readInt()];
      parameters = new Object[parameterClasses.length];
      ObjectWritable objectWritable = new ObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = ObjectWritable.readObject(in, objectWritable);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
      result = ObjectWritable.readObject(in, null);
    }

    public String toString() {
      return toString(false);
    }

    public String toString(boolean withDeclaringClassSimpleName) {
      StringBuffer buffer = new StringBuffer();
      if (getCallerName(null, null, false) != null) buffer.append(getCallerName(null, null, false)).append("|");
      if (elapsedTime >= 0) buffer.append("elapsedTime=").append(elapsedTime).append("ms->");
      if (withDeclaringClassSimpleName) buffer.append(method.getDeclaringClass().getSimpleName()).append('.');
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0) buffer.append(", ");
        buffer.append(Utilities.deepToString(parameters[i]));
      }
      buffer.append(")");
      if (haveInvoked) {
        buffer.append("->");
        if (proxyInvocation == null) {
          if (result == null) {
            try {
              Class<?> returnType = getMethod().getReturnType();
              if (returnType.equals(Void.class) || returnType.equals(Void.TYPE)) buffer.append(returnType
                  .getSimpleName());
              else buffer.append("null");
            } catch (IOException e) {
              buffer.append("fail to get return method type");
            }
          } else buffer.append(Utilities.deepToString(result));
        } else {
          proxyInvocation.setCallerProcessId(0);
          buffer.append(proxyInvocation.toString(withDeclaringClassSimpleName));
        }
      }
      return buffer.toString();
    }
  }

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory if no cached client exists.
     * 
     * @param conf
     *          Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf, SocketFactory factory) {
      // Construct & cache client. The configuration is only used for timeout,
      // and Clients have connection pools. So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for
      // all
      // configurations. Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      Client client = clients.get(factory);
      if (client == null) {
        client = new Client(ObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory if no cached client exists.
     * 
     * @param conf
     *          Configuration
     * @return an IPC client
     */
    private synchronized Client getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection A RPC client is closed only when its reference count becomes zero.
     */
    private void stopClient(Client client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  private static ClientCache CLIENTS = new ClientCache();

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private Client client;
    private boolean isClosed = false;

    public Invoker(InetSocketAddress address, Configuration conf, SocketFactory factory) {
      this.address = address;
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      final boolean logDebug = LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }
      ObjectWritable value = (ObjectWritable) client.call(new Invocation(method, args), address);
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    synchronized private void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  @SuppressWarnings("serial")
  public static class VersionMismatch extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;

    /**
     * Create a version mismatch exception
     * 
     * @param interfaceName
     *          the name of the protocol mismatch
     * @param clientVersion
     *          the client's version of the protocol
     * @param serverVersion
     *          the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion, long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " + clientVersion + ", server = "
          + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }

    /**
     * Get the interface name
     * 
     * @return the java class name (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }

    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }

    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  public static VersionedProtocol waitForProxy(Class<?> protocol, long clientVersion, InetSocketAddress addr,
      Configuration conf) throws IOException {
    return waitForProxy(protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol
   *          protocol class
   * @param clientVersion
   *          client version
   * @param addr
   *          remote address
   * @param conf
   *          configuration to use
   * @param timeout
   *          time in milliseconds before giving up
   * @return the proxy
   * @throws IOException
   *           if the far end through a RemoteException
   */
  static VersionedProtocol waitForProxy(Class<?> protocol, long clientVersion, InetSocketAddress addr,
      Configuration conf, long timeout) throws IOException {
    long startTime = System.currentTimeMillis();
    IOException ioe;
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf);
      } catch (ConnectException se) { // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch (SocketTimeoutException te) { // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      }
      // check if timed out
      if (System.currentTimeMillis() - timeout >= startTime) { throw ioe; }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol, talking to a server at the named address.
   */
  public static VersionedProtocol getProxy(Class<?> protocol, long clientVersion, InetSocketAddress addr,
      Configuration conf, SocketFactory factory) throws IOException {

    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[] { protocol }, new Invoker(
            addr, conf, factory));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(), clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    } else {
      throw new VersionMismatch(protocol.getName(), clientVersion, serverVersion);
    }
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static VersionedProtocol getProxy(Class<?> protocol, long clientVersion, InetSocketAddress addr,
      Configuration conf) throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  /**
   * Stop this proxy and release its invoker's resource
   * 
   * @param proxy
   *          the proxy to be stopped
   */
  public static void stopProxy(VersionedProtocol proxy) {
    if (proxy != null) {
      ((Invoker) Proxy.getInvocationHandler(proxy)).close();
    }
  }

  /** Expert: Make multiple, parallel calls to a set of servers. */
  public static Object[] call(Method method, Object[][] params, InetSocketAddress[] addrs, Configuration conf)
      throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    Client client = CLIENTS.getClient(conf);
    try {
      Writable[] wrappedValues = client.call(invocations, addrs);

      if (method.getReturnType() == Void.TYPE) { return null; }

      Object[] values = (Object[]) Array.newInstance(method.getReturnType(), wrappedValues.length);
      for (int i = 0; i < values.length; i++)
        if (wrappedValues[i] != null) values[i] = ((ObjectWritable) wrappedValues[i]).get();

      return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /**
   * Construct a server for a protocol implementation instance listening on a port and address.
   */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf)
      throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /**
   * Construct a server for a protocol implementation instance listening on a port and address.
   */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
      final int numHandlers, final boolean verbose, Configuration conf) throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose);
  }

  /** An RPC Server. */
  public static class Server extends com.taobao.adfs.distributed.rpc.Server {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;

    /**
     * Construct an RPC server.
     * 
     * @param instance
     *          the instance whose methods will be called
     * @param conf
     *          the configuration to use
     * @param bindAddress
     *          the address to bind on to listen for connection
     * @param port
     *          the port to listen for connections on
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) throws IOException {
      this(instance, conf, bindAddress, port, 1, false);
    }

    private static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) { return className; }
      return names[names.length - 1];
    }

    /**
     * Construct an RPC server.
     * 
     * @param instance
     *          the instance whose methods will be called
     * @param conf
     *          the configuration to use
     * @param bindAddress
     *          the address to bind on to listen for connection
     * @param port
     *          the port to listen for connections on
     * @param numHandlers
     *          the number of method handler threads to run
     * @param verbose
     *          whether each call should be logged
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port, int numHandlers, boolean verbose)
        throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf, classNameBase(instance.getClass().getName()));
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
    }

    @Override
    public Writable call(Writable param, long receivedTime) throws IOException {
      try {
        Invocation call = (Invocation) param;
        if (verbose) log("Call: " + call);
        Method method = implementation.getMethod(call.getMethodName(), call.getParameterClasses());

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        rpcQueueTime = (int) (startTime - receivedTime);
        int rpcProcessingTime = (int) (System.currentTimeMillis() - startTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() + " queueTime= " + rpcQueueTime + " procesingTime= "
              + rpcProcessingTime);
        }

        if (verbose) log("Return: " + value);
        return new ObjectWritable(method.getReturnType(), value);
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException) target;
        } else {
          IOException ioe = new IOException(target.toString());
          ioe.setStackTrace(target.getStackTrace());
          throw ioe;
        }
      } catch (Throwable e) {
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }
  }

  private static void log(String value) {
    if (value != null && value.length() > 55) value = value.substring(0, 55) + "...";
    LOG.info(value);
  }
}
