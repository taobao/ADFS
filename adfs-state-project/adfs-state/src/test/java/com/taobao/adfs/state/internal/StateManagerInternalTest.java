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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.*;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockRepositoryTest;
import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.datanode.DatanodeRepositoryTest;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepositoryTest;
import com.taobao.adfs.state.internal.StateManagerInternal;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerInternalTest {
  static StateManagerInternal stateManagerInternal = null;

  @BeforeClass
  static public void setupAfterClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
    Configuration conf = new Configuration(false);
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", FileRepositoryTest.simulatorDescription
        + BlockRepositoryTest.simulatorDescription + DatanodeRepositoryTest.simulatorDescription);
    conf.set("distributed.data.path", "target/test" + StateManagerInternalTest.class.getSimpleName());
    conf.setInt("file.cache.capacity", 10000);
    LogManager.getLogger(com.taobao.adfs.distributed.DistributedDataCache.class).setLevel(Level.DEBUG);
    stateManagerInternal = new StateManagerInternal(conf, null);
    stateManagerInternal.format();
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (stateManagerInternal != null) stateManagerInternal.close();
  }

  @Before
  public void setupBeforeTest() throws Exception {
    stateManagerInternal.format();
  }

  @Test
  public void insertSameParentFileConcurrently() throws Exception {
    class MyRunnable implements Runnable {
      String path = null;
      Throwable t = null;

      MyRunnable(String path) {
        this.path = path;
      }

      @Override
      public void run() {
        try {
          stateManagerInternal.insertFileByPath(path, 0, -1, (byte) 0, false);
        } catch (Throwable t) {
          t.printStackTrace();
          this.t = t;
        }
      }
    }

    MyRunnable[] myRunnables = new MyRunnable[2];
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(myRunnables.length);
    for (int i = 0; i < myRunnables.length; ++i) {
      myRunnables[i] = new MyRunnable("/folder/file" + i);
    }
    for (int i = 0; i < myRunnables.length; ++i) {
      threadPoolExecutor.execute(myRunnables[i]);
    }
    Thread.sleep(200);
    assertThat(myRunnables[0].t == null, is(true));
  }

  @Test
  public void findAndUpdateFileConcurrently() throws Exception {
    final File file = stateManagerInternal.insertFileByPath("/file", 0, -1, (byte) 0, false)[0];
    class MyRunnable implements Runnable {
      Throwable t = null;
      boolean isFind = true;

      MyRunnable(boolean isFind) {
        this.isFind = isFind;
      }

      @Override
      public void run() {
        try {
          if (isFind) stateManagerInternal.findFileByPath("/file");
          else stateManagerInternal.updateFileByFile(file, -1);
        } catch (Throwable t) {
          t.printStackTrace();
          this.t = t;
        }
      }
    }

    MyRunnable[] myRunnables = new MyRunnable[2];
    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(myRunnables.length);
    myRunnables[0] = new MyRunnable(true);
    myRunnables[1] = new MyRunnable(false);

    for (int i = 0; i < myRunnables.length; ++i) {
      threadPoolExecutor.execute(myRunnables[i]);
    }

    Thread.sleep(200);
    for (int i = 0; i < myRunnables.length; ++i) {
      assertThat(myRunnables[i].t == null, is(true));
    }
  }

  @Test
  public void format() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    stateManagerInternal.insertBlockByBlock(new Block(0, 0, 0, 1, 0, 0, 0));
    stateManagerInternal.insertDatanodeByDatanode(new Datanode());

    stateManagerInternal.format();
    assertThat(stateManagerInternal.getDataVersion() == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/") != null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal.findBlockById(0).length == 0, is(true));
    assertThat(stateManagerInternal.findDatanodeById(0).length == 0, is(true));
  }

  @Test
  public void insertDeleteInsertAndFindFileWithoutException() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true);
    stateManagerInternal.deleteFileByPath("/a", true);
    stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true);
    File file = stateManagerInternal.findFileByPath("/a");
    assertThat(file != null, is(true));
  }

  @Test
  public void insertFileWithoutException() throws Exception {
    stateManagerInternal.insertFileByPath("/", 0, -1, (byte) 0, false);
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    stateManagerInternal.insertFileByPath("/a/a/a", 0, 0, (byte) 0, true);
    assertThat(stateManagerInternal.findFileByPath("/").isDir(), is(true));
    assertThat(stateManagerInternal.findFileByPath("/").owner == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").path, is("/"));
    assertThat(stateManagerInternal.findFileByPath("/a").isDir(), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a").owner == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a").path, is("/a"));
    assertThat(stateManagerInternal.findFileByPath("/a/a").isDir(), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a").owner == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a").path, is("/a/a"));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").isDir(), is(false));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").owner == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").blockSize == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").replication == 0, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").path, is("/a/a/a"));
    assertThat(stateManagerInternal.getDataVersion() == 4, is(true));
    assertThat(stateManagerInternal.getVersionFromDatabase() == 4, is(true));
  }

  @Test
  public void updateFileWithoutException() throws Exception {
    // test update a directory
    File file = stateManagerInternal.findFileByPath("/");
    Thread.sleep(1);// change owner and wait 1s(so mtime will be changed)
    stateManagerInternal.updateFileByPathAndFile("/", new File(0, "", -1, 0, (byte) 0, 0, 0, 1), 0x80);
    assertThat(stateManagerInternal.findFileByPath("/").id == file.id, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").parentId == file.parentId, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").isDir(), is(true));
    assertThat(stateManagerInternal.findFileByPath("/").atime == file.atime, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").mtime > file.mtime, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").owner == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/").path, is("/"));

    // test update a file
    file = stateManagerInternal.insertFileByPath("/a", 0, 0, (byte) 0, true)[0];
    Thread.sleep(1);// wait, so mtime will be changed
    stateManagerInternal.updateFileByPathAndFile("/a", new File(file.parentId, "b", 1, 1, (byte) 1, 0, 0, 1), 0x9f);
    assertThat(stateManagerInternal.findFileByPath("/a") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").id == file.id, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").parentId == file.parentId, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").length == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").blockSize == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").replication == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").atime == file.atime, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").mtime > file.mtime, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").owner == 1, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").path, is("/b"));

    // test rename
    stateManagerInternal.insertFileByPath("/A/A", 0, -1, (byte) 0, false);
    stateManagerInternal.updateFileByPathAndFile("/A/A", new File(0, "B", -1, 0, (byte) 0, 0, 0, 0), 0x02);
    assertThat(stateManagerInternal.findFileByPath("/A/A") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/A/B") != null, is(true));

    // set update atime/mtime
    File[] createdNodes = stateManagerInternal.insertFileByPath("/c", 0, 0, (byte) 0, true);
    file = createdNodes[createdNodes.length - 1];
    stateManagerInternal.updateFileByPathAndFile("/c", new File(0, "", 0, 0, (byte) 0, 123, 456, 1), 0x60);
    assertThat(stateManagerInternal.findFileByPath("/c").atime == 123, is(true));
    assertThat(stateManagerInternal.findFileByPath("/c").mtime == 456, is(true));
  }

  @Test
  public void updateFileAndInsertAgain() throws Exception {
    // test cooperation of delete and insert
    assertThat(stateManagerInternal.insertFileByPath("/0/1/2/3/4/5/6/7/8/9", 0, -1, (byte) 0, false).length, is(10));
    assertThat(stateManagerInternal.updateFileByPathAndPath("/0/1/2/3/4/5/6/7/8/9", "/1").length, is(1));
    assertThat(stateManagerInternal.insertFileByPath("/0/1/2/3/4/5/6/7/8/9", 0, -1, (byte) 0, false).length, is(1));
    assertThat(stateManagerInternal.findFileByPath("/0/1/2/3/4/5/6/7/8/9") != null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/1") != null, is(true));
  }

  @Test
  public void updateFileWithExceptionForMoveRoot() throws Exception {
    try {
      stateManagerInternal.updateFileByPathAndFile("/", new File(0, "a", -1, 0, (byte) 0, 0, 0, 0), 0x02);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateFileWithExceptionForEmptyNameAndNotRoot() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    try {
      stateManagerInternal.updateFileByPathAndFile("/a", new File(0, "", -1, 0, (byte) 0, 0, 0, 0), 0x02);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateFileWithExceptionForChangeType() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    try {
      stateManagerInternal.updateFileByPathAndFile("/", new File(0, "", 0, 0, (byte) 0, 0, 0, 0), 0x04);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateFileWithExceptionForMoveTargetParentIsNotExisted() throws Exception {
    stateManagerInternal.insertFileByPath("/a/b", 0, -1, (byte) 0, false);
    int id = File.ROOT.id;
    while (stateManagerInternal.findFileById(++id) != null);
    try {
      stateManagerInternal.updateFileByPathAndFile("/a/b", new File(0, "b", 0, 0, (byte) 0, 0, 0, 0), 0x01);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateFileWithExceptionForMoveTargetParentIsFile() throws Exception {
    stateManagerInternal.insertFileByPath("/a/b", 0, -1, (byte) 0, false);
    File file = stateManagerInternal.insertFileByPath("/a/B", 0, 0, (byte) 0, true)[0];
    try {
      stateManagerInternal.updateFileByPathAndFile("/a/b", new File(file.id, "b", 0, 0, (byte) 0, 0, 0, 0), 0x01);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void deleteFileWithoutException() throws Exception {
    File[] results = stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    File file = results[results.length - 1];
    File[] files = stateManagerInternal.deleteFileByPath("/a", false);
    assertThat(stateManagerInternal.findFileByPath("/a") == null, is(true));
    assertThat(files.length == 1, is(true));
    assertThat(files[0].version != file.version, is(true));
    files[0].version = file.version;
    assertThat(files[0].equals(file), is(true));

    file = stateManagerInternal.insertFileByPath("/b/b", 0, -1, (byte) 0, false)[1];
    files = stateManagerInternal.deleteFileByPath("/b", true);
    assertThat(stateManagerInternal.findFileByPath("/b") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/b/b") == null, is(true));
    assertThat(files.length == 2, is(true));
    assertThat(files[0].name.equals("b"), is(true));
    assertThat(files[1].name.equals("b"), is(true));
  }

  @Test
  public void deleteFileAndInsertAgain() throws Exception {
    // test cooperation of delete and insert
    assertThat(stateManagerInternal.insertFileByPath("/0/1/2/3/4/5/6/7/8/9", 0, -1, (byte) 0, false).length, is(10));
    assertThat(stateManagerInternal.deleteFileByPath("/0", true).length, is(10));
    assertThat(stateManagerInternal.insertFileByPath("/0/1/2/3/4/5/6/7/8/9", 0, -1, (byte) 0, false).length, is(10));
    assertThat(stateManagerInternal.findFileByPath("/0/1/2/3/4/5/6/7/8/9") != null, is(true));
  }

  @Test
  public void deleteFileByFileWithoutException() throws Exception {
    assertThat(stateManagerInternal.insertFileByPath("/dir1/1/a", 0, -1, (byte) 0, false).length == 3, is(true));
    assertThat(stateManagerInternal.insertFileByPath("/dir1/1/b", 0, -1, (byte) 0, false).length == 1, is(true));
    assertThat(stateManagerInternal.insertFileByPath("/dir1/2/c", 0, -1, (byte) 0, false).length == 2, is(true));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(7));
    try {
      stateManagerInternal.deleteFileByFile(stateManagerInternal.findFileByPath("/dir1"), false);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(7));
    assertThat(stateManagerInternal.deleteFileByFile(stateManagerInternal.findFileByPath("/dir1"), true).length, is(6));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(1));

    assertThat(stateManagerInternal.insertFileByPath("/dir2/1/a", 0, -1, (byte) 0, false).length, is(3));
    assertThat(stateManagerInternal.insertFileByPath("/dir2/1/b", 0, -1, (byte) 0, false).length, is(1));
    assertThat(stateManagerInternal.insertFileByPath("/dir2/2/c", 0, -1, (byte) 0, false).length, is(2));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(7));
    File[] filesToDelete =
        new File[] { stateManagerInternal.findFileByPath("/dir2/1"), stateManagerInternal.findFileByPath("/dir2/2") };
    try {
      stateManagerInternal.deleteFileByFiles(filesToDelete, false);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(7));
    assertThat(stateManagerInternal.deleteFileByFiles(filesToDelete, true).length, is(5));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length, is(2));
  }

  @Test
  public void deleteFileWithExceptionForFoundChildrenAndNotRecursively() throws Exception {
    stateManagerInternal.insertFileByPath("/a", 0, -1, (byte) 0, false);
    try {
      stateManagerInternal.deleteFileByPath("/", false);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void findFileWithoutException() throws Exception {
    stateManagerInternal.insertFileByPath("/a/a/a", 0, -1, (byte) 0, false);
    stateManagerInternal.insertFileByPath("/b", 0, -1, (byte) 0, false);
    stateManagerInternal.insertFileByPath("/c", 0, 0, (byte) 0, true);
    assertThat(stateManagerInternal.findFileByPath("/").name.equals(File.ROOT.name), is(true));
    assertThat(stateManagerInternal.findFileByPath("/").path, is("/"));
    assertThat(stateManagerInternal.findFileByPath("/a").name.equals("a"), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a").path, is("/a"));
    assertThat(stateManagerInternal.findFileByPath("/a/a").name.equals("a"), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a").path, is("/a/a"));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").name.equals("a"), is(true));
    assertThat(stateManagerInternal.findFileByPath("/a/a/a").path, is("/a/a/a"));
    assertThat(stateManagerInternal.findFileByPath("/b").name.equals("b"), is(true));
    assertThat(stateManagerInternal.findFileByPath("/b").path, is("/b"));
    assertThat(stateManagerInternal.findFileByPath("/c").name.equals("c"), is(true));
    assertThat(stateManagerInternal.findFileByPath("/c").path, is("/c"));
    assertThat(stateManagerInternal.findFileByPath("/b/b/b") == null, is(true));
    assertThat(stateManagerInternal.findFileByPath("/a").equals(
        stateManagerInternal.findFileById(stateManagerInternal.findFileByPath("/a").id)), is(true));
    assertThat(stateManagerInternal.findFileChildrenByPath("/").length == 3, is(true));
    assertThat(stateManagerInternal.findFileChildrenByPath("/notExistedDir") == null, is(true));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", false, false).length == 6, is(true));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", true, false).length == 1, is(true));
    assertThat(
        stateManagerInternal.findFileDescendantByFile(stateManagerInternal.findFileByPath("/"), false, false).length,
        is(6));
    assertThat(
        stateManagerInternal.findFileDescendantByFile(stateManagerInternal.findFileByPath("/"), true, false).length,
        is(1));
    assertThat(stateManagerInternal.findFileDescendantByPath("/", true, true).length, is(2));
    assertThat(
        stateManagerInternal.findFileDescendantByFile(stateManagerInternal.findFileByPath("/"), true, true).length,
        is(2));
  }

  @Test
  public void findFileWithExceptionForInvalidPath() throws Exception {
    try {
      stateManagerInternal.findFileByPath("invalid/path");
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void findInsertUpdateAndDeleteBlock() throws Exception {
    // test block find and insert
    Block block = new Block();
    block.id = 1L;
    block.datanodeId = 1;
    block.fileId = 1;
    block.numbytes = 1L;
    block.generationStamp = 1L;
    block.fileIndex = 1;
    Block[] results = stateManagerInternal.insertBlockByBlock(block);
    assertThat(results.length == 1, is(true));
    block.version = results[0].version;
    assertThat(stateManagerInternal.findRowByVersion(2).equals(block), is(true));
    assertThat(stateManagerInternal.findBlockById(1)[0].equals(block), is(true));
    assertThat(stateManagerInternal.findBlockByDatanodeId(1)[0].equals(block), is(true));
    assertThat(stateManagerInternal.findBlockByFileId(1)[0].equals(block), is(true));

    // test block update
    block.fileId = 2;
    block.numbytes = 2L;
    block.generationStamp = 2L;
    block.fileIndex = 2;
    results = stateManagerInternal.updateBlockByBlock(block);
    assertThat(results.length == 1, is(true));
    block.version = results[0].version;
    assertThat(stateManagerInternal.findRowByVersion(3).equals(block), is(true));
    assertThat(stateManagerInternal.findBlockById(1)[0].equals(block), is(true));

    // test block delete
    block.id = 3L;
    block.datanodeId = 3;
    block.fileId = 3;
    results = stateManagerInternal.insertBlockByBlock(block);
    block.version = results[0].version;
    assertThat(stateManagerInternal.findRowByVersion(4).equals(block), is(true));
    block.id = 4L;
    block.datanodeId = 4;
    block.fileId = 4;
    results = stateManagerInternal.insertBlockByBlock(block);
    block.version = results[0].version;
    assertThat(stateManagerInternal.findRowByVersion(5).equals(block), is(true));
    block.id = 5L;
    block.datanodeId = 5;
    block.fileId = 5;
    results = stateManagerInternal.insertBlockByBlock(block);
    block.version = results[0].version;
    assertThat(stateManagerInternal.findRowByVersion(6).equals(block), is(true));
    // delete block
    stateManagerInternal.deleteBlockById(3);
    assertThat(stateManagerInternal.findBlockById(3).length == 0, is(true));
    stateManagerInternal.deleteBlockByIdAndDatanodeId(4, 4);
    assertThat(stateManagerInternal.findBlockByDatanodeId(4).length == 0, is(true));
    stateManagerInternal.deleteBlockByFileId(5);
    assertThat(stateManagerInternal.findBlockByFileId(5).length == 0, is(true));
  }

  @Test
  public void findBlockByFiles() throws Exception {
    stateManagerInternal.insertFileByPath("/file0", 0, 0, (byte) 0, true);
    stateManagerInternal.insertFileByPath("/file1", 0, 0, (byte) 0, true);
    File file0 = stateManagerInternal.findFileByPath("/file0");
    File file1 = stateManagerInternal.findFileByPath("/file1");

    Block block = new Block();
    block.id = 0L;
    block.datanodeId = 0;
    block.fileId = file0.id;
    block.numbytes = 0;
    block.generationStamp = 0L;
    block.fileIndex = 0;
    stateManagerInternal.insertBlockByBlock(block);
    block.datanodeId = 1;
    stateManagerInternal.insertBlockByBlock(block);
    block.id = 1;
    block.datanodeId = 0;
    block.fileId = file1.id;
    stateManagerInternal.insertBlockByBlock(block);

    File[] files = new File[] { stateManagerInternal.findFileByPath("/") };
    assertThat(stateManagerInternal.findBlockByFiles(files).length, is(3));
  }

  @Test
  public void findDatanodeByUpdateTime() throws Exception {
    Datanode datanode = new Datanode();
    datanode.id = 0;
    datanode.lastUpdated = System.currentTimeMillis();
    stateManagerInternal.insertDatanodeByDatanode(datanode);
    assertThat(stateManagerInternal.findDatanodeByUpdateTime(0).length > 0, is(true));
    assertThat(stateManagerInternal.findDatanodeByUpdateTime(Long.MAX_VALUE).length == 0, is(true));
  }

  @Test
  public void findDatanodeByStorageId() throws Exception {
    Datanode datanode = new Datanode();
    datanode.id = 0;
    datanode.storageId = "storageId";
    stateManagerInternal.insertDatanodeByDatanode(datanode);
    assertThat(stateManagerInternal.findDatanodeByStorageId(datanode.storageId).length > 0, is(true));
  }

  @Test
  public void insertDatanode() throws Exception {
    Datanode datanode = new Datanode();
    datanode.id = 0;
    datanode.capacity = 0L;
    datanode.ctime = 0L;
    datanode.dfsUsed = 0L;
    datanode.infoPort = 0;
    datanode.ipcPort = 0;
    datanode.lastUpdated = System.currentTimeMillis();
    datanode.layoutVersion = 0;
    datanode.location = "location";
    datanode.name = "name";
    datanode.namespaceId = 0;
    datanode.remaining = 0L;
    datanode.storageId = "storageId";
    datanode.xceiverCount = 0;
    datanode.hostName = "0.0.0.1";
    datanode.adminState = 0;
    stateManagerInternal.insertDatanodeByDatanode(datanode);
    Datanode[] datanodes = stateManagerInternal.findDatanodeByUpdateTime(0);
    datanode.version = datanodes[0].version;
    assertThat(datanodes.length, is(1));
    assertThat(datanodes[0].equals(datanode), is(true));
  }

  @Test
  public void updateDatanode() throws Exception {
    // make sure IOException is thrown when handle heart beat before register data node
    Datanode datanode = new Datanode();
    datanode.id = 0;
    try {
      stateManagerInternal.updateDatanodeByDatanode(datanode);
      throw new Exception();
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
      stateManagerInternal.insertDatanodeByDatanode(datanode);
    }

    datanode.capacity = 1024L;
    datanode.dfsUsed = 512L;
    datanode.remaining = datanode.capacity - datanode.dfsUsed;
    datanode.xceiverCount = 3;
    datanode.lastUpdated = System.currentTimeMillis();
    stateManagerInternal.updateDatanodeByDatanode(datanode);
    Datanode[] datanodes = stateManagerInternal.findDatanodeByUpdateTime(0);
    assertThat(datanodes.length, is(1));
    assertThat(datanodes[0].capacity == datanode.capacity, is(true));
    assertThat(datanodes[0].dfsUsed == datanode.dfsUsed, is(true));
    assertThat(datanodes[0].remaining == datanode.remaining, is(true));
    assertThat(datanodes[0].xceiverCount == datanode.xceiverCount, is(true));
    assertThat(datanodes[0].lastUpdated == datanode.lastUpdated, is(true));
  }
}
