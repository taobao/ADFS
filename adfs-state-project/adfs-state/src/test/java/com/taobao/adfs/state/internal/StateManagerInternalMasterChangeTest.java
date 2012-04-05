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

package com.taobao.adfs.state.internal;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.distributed.DistributedDataCache;
import com.taobao.adfs.distributed.DistributedServer;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepositoryTest;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerInternalMasterChangeTest implements InvocationHandler {
  protected static StateManagerInternal stateManagerInternal0 = null;
  protected static StateManagerInternal stateManagerInternal1 = null;
  protected static StateManagerInternalProtocol stateManagerInternalProxy = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
    Utilities.setLoggerLevel(DistributedDataCache.class.getName(), Level.DEBUG.toString(), null);
    Configuration conf0 = new Configuration(false);
    conf0.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf0.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf0.setInt("file.cache.capacity", 10000);
    conf0.set("distributed.data.path", "target/test" + StateManagerInternalTest.class.getSimpleName() + "0");
    stateManagerInternal0 = new StateManagerInternal(conf0, null);
    Configuration conf1 = new Configuration(conf0);
    conf1.set("distributed.data.path", "target/test" + StateManagerInternalTest.class.getSimpleName() + "1");
    stateManagerInternal1 = new StateManagerInternal(conf1, null);
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (stateManagerInternal0 != null) stateManagerInternal0.close();
    if (stateManagerInternal1 != null) stateManagerInternal1.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    if (stateManagerInternalProxy == null)
      stateManagerInternalProxy =
          (StateManagerInternalProtocol) Proxy.newProxyInstance(getClass().getClassLoader(), StateManagerInternal.class
              .getInterfaces(), this);
    DistributedServer.getThreadLocalInvocation().set(null);
    stateManagerInternal0.format();
    stateManagerInternal1.format();
  }

  static AtomicLong invocationCounter = new AtomicLong(0);

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Invocation invocation = new Invocation(method, args);
    invocation.setCallerAddress("127.0.0.1");
    invocation.setCallerProcessId(Utilities.getPid());
    invocation.setCallerThreadId(Thread.currentThread().getId());
    invocation.setCallerThreadName(Thread.currentThread().getName());
    invocation.setCallerSequenceNumber(invocationCounter.getAndIncrement());
    DistributedServer.getThreadLocalInvocation().set(invocation);
    Object result0 = stateManagerInternal0.invoke(invocation);
    Object result1 = stateManagerInternal1.invoke(invocation);
    assertThat(Utilities.deepEquals(result0, result1), is(true));
    invocation.resetResult();
    result1 = stateManagerInternal1.invoke(invocation);
    if (method.getName().equals("unlock")) {
      assertThat(result1 == null, is(true));
    } else if (method.getName().startsWith("delete")) {
      assertThat(Array.getLength(result1) == 0, is(true));
    } else {
      assertThat(Utilities.deepEquals(result0, result1), is(true));
    }
    return result0;
  }

  @Test
  public void testLockerLock() throws Exception {
    stateManagerInternalProxy.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, 0);
    assertThat(stateManagerInternal0.getDataLocker().getLock(0) != null, is(true));
    assertThat(stateManagerInternal0.getDataLocker().getLock(0)
        .equals(stateManagerInternal1.getDataLocker().getLock(0)), is(true));
  }

  @Test
  public void testLockerTryLock() throws Exception {
    stateManagerInternalProxy.tryLock(null, Long.MAX_VALUE, 0);
    assertThat(stateManagerInternal0.getDataLocker().getLock(0) != null, is(true));
    assertThat(stateManagerInternal0.getDataLocker().getLock(0)
        .equals(stateManagerInternal1.getDataLocker().getLock(0)), is(true));
  }

  @Test
  public void testLockerUnlock() throws Exception {
    stateManagerInternalProxy.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, 0);
    stateManagerInternalProxy.unlock(null, 0);
    assertThat(stateManagerInternal0.getDataLocker().getLock(0) == null, is(true));
    assertThat(stateManagerInternal0.getDataLocker().getLock(0) == null, is(true));
  }

  @Test
  public void testFileInsertFileByPath() throws Exception {
    stateManagerInternalProxy.insertFileByPath("/a/b/c/d", 0, 0, (byte) 0, true);
    File[] files0 = stateManagerInternal0.findFileDescendantByPath("/a", false, true);
    File[] files1 = stateManagerInternal1.findFileDescendantByPath("/a", false, true);
    assertThat(files0 != null, is(true));
    assertThat(Arrays.deepEquals(files0, files1), is(true));
    stateManagerInternalProxy.insertFileByPath("/A/B/C/D", 0, 0, (byte) 0, false);
    files0 = stateManagerInternal0.findFileDescendantByPath("/A", false, true);
    files1 = stateManagerInternal1.findFileDescendantByPath("/A", false, true);
    assertThat(files0 != null, is(true));
    assertThat(Arrays.deepEquals(files0, files1), is(true));
  }

  @Test
  public void testFileUpdateFileByFile() throws Exception {
    File file = stateManagerInternal0.findFileByPath("/");
    file.owner = 1;
    stateManagerInternalProxy.updateFileByFile(file, -1);
    assertThat(stateManagerInternal0.findFileByPath("/") != null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/").equals(stateManagerInternal1.findFileByPath("/")), is(true));
  }

  @Test
  public void testFileUpdateFileByPathAndFile() throws Exception {
    File file = stateManagerInternal0.findFileByPath("/");
    file.owner = 1;
    stateManagerInternalProxy.updateFileByPathAndFile("/", file, -1);
    assertThat(stateManagerInternal0.findFileByPath("/") != null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/").equals(stateManagerInternal1.findFileByPath("/")), is(true));
  }

  @Test
  public void testFileUpdateFileByPathAndPath() throws Exception {
    stateManagerInternalProxy.insertFileByPath("/a", 0, 0, (byte) 0, true);
    stateManagerInternalProxy.updateFileByPathAndPath("/a", "/b");
    assertThat(stateManagerInternal0.findFileByPath("/b") != null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/b").equals(stateManagerInternal1.findFileByPath("/b")), is(true));
    assertThat(stateManagerInternal0.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a") == null, is(true));
  }

  @Test
  public void testFileDeleteFileByPath() throws Exception {
    stateManagerInternalProxy.insertFileByPath("/a/b", 0, 0, (byte) 0, true);
    stateManagerInternalProxy.insertFileByPath("/A", 0, 0, (byte) 0, true);
    stateManagerInternalProxy.deleteFileByPath("/a", true);
    assertThat(stateManagerInternal0.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/a/b") == null, is(true));
    stateManagerInternalProxy.deleteFileByPath("/A", false);
    assertThat(stateManagerInternal0.findFileByPath("/A") == null, is(true));
  }

  @Test
  public void testFileDeleteFileByFile() throws Exception {
    File[] files = stateManagerInternalProxy.insertFileByPath("/a/b", 0, 0, (byte) 0, true);
    stateManagerInternalProxy.deleteFileByFile(files[1], false);
    stateManagerInternalProxy.deleteFileByFile(files[0], true);
    assertThat(stateManagerInternal0.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a") == null, is(true));
    files = stateManagerInternalProxy.insertFileByPath("/A/B", 0, 0, (byte) 0, true);
    stateManagerInternalProxy.deleteFileByFile(files[0], true);
    assertThat(stateManagerInternal0.findFileByPath("/A/B") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/A") == null, is(true));
  }

  @Test
  public void testFileDeleteFileByFiles() throws Exception {
    File[] files0 = stateManagerInternalProxy.insertFileByPath("/a/b", 0, 0, (byte) 0, true);
    File[] files1 = stateManagerInternalProxy.insertFileByPath("/A/B", 0, 0, (byte) 0, true);
    File[] filesToDelete = new File[] { files0[1], files0[0], files1[1], files1[0] };
    stateManagerInternalProxy.deleteFileByFiles(filesToDelete, false);
    assertThat(stateManagerInternal0.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/A") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/A/B") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/A") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/A/B") == null, is(true));
    files0 = stateManagerInternalProxy.insertFileByPath("/0/a", 0, 0, (byte) 0, true);
    files1 = stateManagerInternalProxy.insertFileByPath("/1/A", 0, 0, (byte) 0, true);
    filesToDelete = new File[] { files0[0], files1[0] };
    stateManagerInternalProxy.deleteFileByFiles(filesToDelete, true);
    assertThat(stateManagerInternal0.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/A") == null, is(true));
    assertThat(stateManagerInternal0.findFileByPath("/A/B") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a/b") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/A") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/A/B") == null, is(true));
  }

  @Test
  public void testBlockInsertBlockByBlock() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternalProxy.insertBlockByBlock(block);
    assertThat(stateManagerInternal0.findBlockById(0)[0] != null, is(true));
    assertThat(stateManagerInternal0.findBlockById(0)[0].equals(stateManagerInternal1.findBlockById(0)[0]), is(true));
  }

  @Test
  public void testBlockUpdateBlockByBlock() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternalProxy.insertBlockByBlock(block);
    block.fileId = 1;
    stateManagerInternalProxy.updateBlockByBlock(block);
    assertThat(stateManagerInternal0.findBlockById(0)[0] != null, is(true));
    assertThat(stateManagerInternal0.findBlockById(0)[0].fileId == 1, is(true));
    assertThat(stateManagerInternal0.findBlockById(0)[0].equals(stateManagerInternal1.findBlockById(0)[0]), is(true));
  }

  @Test
  public void testBlockDeleteBlockById() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternalProxy.insertBlockByBlock(block);
    block.fileId = 1;
    stateManagerInternalProxy.deleteBlockById(0);
    assertThat(stateManagerInternal0.findBlockById(0).length == 0, is(true));
    assertThat(stateManagerInternal1.findBlockById(0).length == 0, is(true));
  }

  @Test
  public void testBlockDeleteBlockByIdAndDatanodeId() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternalProxy.insertBlockByBlock(block);
    block.fileId = 1;
    stateManagerInternalProxy.deleteBlockByIdAndDatanodeId(0, 0);
    assertThat(stateManagerInternal0.findBlockById(0).length == 0, is(true));
    assertThat(stateManagerInternal1.findBlockById(0).length == 0, is(true));
  }

  @Test
  public void testBlockDeleteBlockByFileId() throws Exception {
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    stateManagerInternalProxy.insertBlockByBlock(block);
    block.fileId = 1;
    stateManagerInternalProxy.deleteBlockByFileId(0);
    assertThat(stateManagerInternal0.findBlockById(0).length == 0, is(true));
    assertThat(stateManagerInternal1.findBlockById(0).length == 0, is(true));
  }

  @Test
  public void testDatanodeInsertDatanodeByDatanode() throws Exception {
    Datanode datanode = new Datanode();
    datanode.id = 0;
    stateManagerInternalProxy.insertDatanodeByDatanode(datanode);
    assertThat(stateManagerInternal0.findDatanodeById(0).length == 1, is(true));
    assertThat(stateManagerInternal0.findDatanodeById(0)[0].equals(stateManagerInternal1.findDatanodeById(0)[0]),
        is(true));
  }

  @Test
  public void testDatanodeUpdateDatanodeByDatanode() throws Exception {
    Datanode datanode = new Datanode();
    datanode.id = 0;
    stateManagerInternalProxy.insertDatanodeByDatanode(datanode);
    datanode.hostName = "zhangwei.yangjie@gmail.com";
    stateManagerInternalProxy.updateDatanodeByDatanode(datanode);
    assertThat(stateManagerInternal0.findDatanodeById(0).length == 1, is(true));
    assertThat(stateManagerInternal0.findDatanodeById(0)[0].hostName.equals("zhangwei.yangjie@gmail.com"), is(true));
    assertThat(stateManagerInternal0.findDatanodeById(0)[0].equals(stateManagerInternal1.findDatanodeById(0)[0]),
        is(true));
  }

  @Test
  public void testStateManagerInternalDeleteFileAndBlockByPath() throws Exception {
    stateManagerInternalProxy.insertFileByPath("/a", 0, 0, (byte) 0, true);
    Block block = new Block();
    block.id = 0;
    block.datanodeId = 0;
    block.fileId = stateManagerInternal0.findFileByPath("/a").id;
    stateManagerInternalProxy.insertBlockByBlock(block);
    stateManagerInternalProxy.deleteFileAndBlockByPath("/a", false);
    assertThat(stateManagerInternal0.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal1.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal0.findBlockByFileId(block.fileId).length == 0, is(true));
    assertThat(stateManagerInternal1.findBlockByFileId(block.fileId).length == 0, is(true));
  }
}
