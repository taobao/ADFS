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

package com.taobao.adfs.file;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.junit.*;

import com.taobao.adfs.database.DatabaseExecutorForHandlerSocketSimulator;
import com.taobao.adfs.distributed.test.DistributedDataRepositoryBaseOnTableTest;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepository;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class FileRepositoryTest extends DistributedDataRepositoryBaseOnTableTest {
  public static String simulatorDescription =
      "nn_state.file:id=integer|parentId=integer|name=string|length=integer|blockSize=integer|replication=integer|atime=integer|mtime=integer|owner=integer|version=integer|operateIdentifier=string:PRIMARY=0|PID_NAME=1,2|VERSION=9;";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.configureLog4j(null, "distributed.logger.conf.", Level.DEBUG);
  }

  public Configuration createConf() {
    Configuration conf = new Configuration(false);
    conf.set("distributed.data.repository.class.name", FileRepository.class.getName());
    conf.set("database.executor.class.name", DatabaseExecutorForHandlerSocketSimulator.class.getName());
    conf.set("database.executor.handlersocket.simulator.description", simulatorDescription);
    conf.setInt("file.cache.capacity", 10);
    conf.setInt("file.cache.capacity", 10);
    conf.setInt("file.name.length.max", File.nameMaxLength + 1);
    conf.set("distributed.data.path", "target/test" + getClass().getSimpleName());
    conf.setLong("distributed.data.delete.check.interval.time", 1);
    // conf.set("database.executor.class.name", DatabaseExecutorForMysqlClient.class.getName());
    // conf.set("mysql.server.conf.mysqld.bind-address", "127.0.0.1");
    // conf.set("mysql.server.conf.mysqld.port", "40012");
    return conf;
  }

  @AfterClass
  static public void cleanupAfterClass() throws Exception {
    if (repositories.get(FileRepositoryTest.class) != null) repositories.get(FileRepositoryTest.class).close();
  }

  public FileRepository getRepository() throws Exception {
    return (FileRepository) super.getRepository();
  }

  @Test
  public void insertWithoutException() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a file
    File file = getRepository().insert(new File(rootFile.id, "a", 0, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 2, is(true));
    assertThat(getRepository().findById(file.id).isDir(), is(false));
    assertThat(getRepository().findById(file.id).equals(file), is(true));
    assertThat(file.equals(getRepository().findById(file.id)), is(true));
    assertThat(file.equals(getRepository().findByParentIdAndName(rootFile.id, "a")), is(true));

    // create "/b" as a directory
    file = getRepository().insert(new File(rootFile.id, "b", -1, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 3, is(true));
    assertThat(getRepository().findById(file.id).isDir(), is(true));
    assertThat(getRepository().findById(file.id).equals(file), is(true));
    assertThat(file.equals(getRepository().findById(file.id)), is(true));
    assertThat(file.equals(getRepository().findByParentIdAndName(rootFile.id, "b")), is(true));
  }

  @Test
  public void operateWithExceptionForTooLargeFileName() throws Exception {
    String fileName = Utilities.getString('张', File.nameMaxLength + 1);
    Throwable throwable = null;
    try {
      getRepository().findByParentIdAndName(0, fileName);
    } catch (Throwable t) {
      throwable = t;
    }
    assertThat(throwable instanceof IOException, is(true));
    throwable = null;
    try {
      getRepository().insert(new File(0, fileName), false);
    } catch (Throwable t) {
      throwable = t;
    }
    assertThat(throwable instanceof IOException, is(true));
    throwable = null;
    try {
      getRepository().update(new File(0, fileName), -1);
    } catch (Throwable t) {
      throwable = t;
    }
    assertThat(throwable instanceof IOException, is(true));
    throwable = null;
  }

  @Test
  public void insertWithExceptionForExistedFile() throws Exception {
    try {
      // create "ROOT" as a directory
      getRepository().insert(File.ROOT, false);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void insertWithExceptionForEmptyName() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    try {
      // create child file of root file with empty name
      getRepository().insert(new File(rootFile.id, "", -1, 0, (byte) 0, 0, 0, 0), false);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void insertWithExceptionForNoParent() throws Exception {
    File file = new File(0, "a", 0, 0, (byte) 0, 0, 0, 0);
    // get id of not existed file
    while (getRepository().findById(file.parentId) != null) {
      ++file.parentId;
    }
    try {
      // create file whit not existed parent file
      getRepository().insert(file, false);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void insertWithExceptionForParentIsFile() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a file
    File file = getRepository().insert(new File(rootFile.id, "a", 0, 0, (byte) 0, 0, 0, 0), false);
    try {
      // create "/a/b" as a file
      getRepository().insert(new File(file.id, "b", 0, 0, (byte) 0, 0, 0, 0), false);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateWithoutException() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 2, is(true));
    // update "/a" with new bolockSize/replication/atime/mtime/owner
    file = getRepository().update(new File(file.id, rootFile.id, "a", -1, 1, (byte) 1, 1, 1, 1), -1);
    assertThat(getRepository().findById(file.id).version == 3, is(true));
    assertThat(getRepository().findById(file.id).equals(file), is(true));

    // create "/b" as a file
    file = getRepository().insert(new File(rootFile.id, "b", 0, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 4, is(true));
    // update "/b" with new length/bolockSize/replication/atime/mtime/owner
    file = getRepository().update(new File(file.id, rootFile.id, "b", 1, 1, (byte) 1, 1, 1, 1), -1);
    assertThat(getRepository().findById(file.id).version == 5, is(true));
    assertThat(getRepository().findById(file.id).equals(file), is(true));

    // create "/c" as a directory
    file = getRepository().insert(new File(rootFile.id, "c", -1, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 6, is(true));
    // create "/c/c" as a file
    file = getRepository().insert(new File(file.id, "c", 0, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 7, is(true));
    // update "/c/c" as "/d" with new length/bolockSize/replication/atime/mtime/owner
    file = getRepository().update(new File(file.id, rootFile.id, "d", 1, 1, (byte) 1, 1, 1, 1), -1);
    assertThat(getRepository().findById(file.id).version == 8, is(true));
    assertThat(getRepository().findByParentIdAndName(rootFile.id, "d").equals(file), is(true));
    File cFile = getRepository().findByParentIdAndName(rootFile.id, "c");
    if (cFile == null) {
      String haha = getRepository().toString();
      System.out.println(haha);
      System.out.println(haha);
    }
    assertThat(getRepository().findByParentId(cFile.id).isEmpty(), is(true));
  }

  @Test
  public void updateWithExceptionForNotExisted() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    try {
      // update not existed "/a"
      getRepository().update(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), -1);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateWithExceptionForNoParent() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
    // get id of not existed file
    while (getRepository().findById(file.parentId) != null) {
      ++file.parentId;
    }
    try {
      // update file with not existed parent id
      getRepository().update(file, -1);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateWithExceptionForMoveRoot() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", 0, 0, (byte) 0, 0, 0, 0), false);
    try {
      rootFile.parentId = file.id;
      getRepository().update(rootFile, -1);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateWithExceptionForParentIsSelf() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
    try {
      // update parent id of "/a"
      file.parentId = file.id;
      getRepository().update(file, -1);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void updateWithExceptionForChangeType() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
    try {
      // update parent id of "/a"
      file.length = 0;
      getRepository().update(file, -1);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void deleteWithoutException() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);

    // create "/a" as a file
    File file = getRepository().insert(new File(rootFile.id, "a", 0, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findById(file.id).version == 2, is(true));
    assertThat(getRepository().findByParentId(rootFile.id).size() == 1, is(true));
    // delete "/a"
    File deletedFile = getRepository().delete(file);
    assertThat(deletedFile.version == -3, is(true));
    assertThat(getRepository().findByKeys(deletedFile.getKeys()) == null, is(true));
    deletedFile.version = file.version;
    assertThat(deletedFile.equals(file), is(true));
    assertThat(getRepository().findById(file.id) == null, is(true));
    assertThat(getRepository().findByParentIdAndName(file.parentId, file.name) == null, is(true));
    assertThat(getRepository().findByParentId(rootFile.id).size() == 0, is(true));

    // create "/b" as a directory
    file = getRepository().insert(new File(rootFile.id, "b", -1, 0, (byte) 0, 0, 0, 0), false);
    assertThat(getRepository().findByParentId(rootFile.id).size() == 1, is(true));
    assertThat(getRepository().findById(file.id).version == 4, is(true));
    // delete "/b"
    File deletedDir = getRepository().delete(file);
    assertThat(deletedDir.version == -5, is(true));
    assertThat(getRepository().findByKeys(deletedDir.getKeys()) == null, is(true));
    deletedDir.version = file.version;
    assertThat(deletedDir.equals(file), is(true));
    assertThat(getRepository().findById(file.id) == null, is(true));
    assertThat(getRepository().findByParentIdAndName(file.parentId, file.name) == null, is(true));
    assertThat(getRepository().findByParentId(rootFile.id).size() == 0, is(true));
  }

  @Test
  public void deleteWithExceptionForDeleteNotEmptyDir() throws Exception {
    try {
      // create "/a" as a directory
      File file = getRepository().insert(new File(File.ROOT.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
      // create file "/a/1"
      getRepository().insert(new File(file.id, "1", 0, 0, (byte) 0, 0, 0, 0), false);
      // delete "/a"
      getRepository().delete(file);
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void deleteWithExceptionForDeleteRootFile() throws Exception {
    try {
      getRepository().delete(getRepository().findById(File.ROOT.id));
    } catch (Throwable t) {
      assertThat(t instanceof IOException, is(true));
    }
  }

  @Test
  public void getWithoutException() throws Exception {
    // get root
    File rootFile = getRepository().findById(File.ROOT.id);
    assertThat(rootFile.id == File.ROOT.id, is(true));
    assertThat(rootFile.parentId == File.ROOT.parentId, is(true));
    assertThat(rootFile.name.equals(File.ROOT.name), is(true));
    assertThat(rootFile.isDir(), is(true));
    assertThat(getRepository().findByParentId(File.ROOT.id).size() == 0, is(true));
    assertThat(rootFile.equals(getRepository().findByParentIdAndName(File.ROOT.parentId, File.ROOT.name)), is(true));

    // create "/a" as a directory
    File file = getRepository().insert(new File(rootFile.id, "a", -1, 0, (byte) 0, 0, 0, 0), false);
    // get "/a" by (id) and (parentId, name)
    assertThat(file.equals(getRepository().findById(file.id)), is(true));
    assertThat(file.equals(getRepository().findByParentIdAndName(file.parentId, file.name)), is(true));

    // get children of "/"
    assertThat(getRepository().findByParentId(rootFile.id).size() == 1, is(true));
  }

  @Test
  public void format() throws Exception {
    File rootFile = getRepository().findById(File.ROOT.id);
    // create "/a" as a file
    getRepository().insert(new File(rootFile.id, "a", 0, 0, (byte) 0, 0, 0, 0), false);
    getRepository().getVersion().set(0);
    getRepository().format();
    // get children of "/"
    assertThat(File.ROOT.name.isEmpty(), is(true));
    assertThat(getRepository().findById(File.ROOT.id).version == 1, is(true));
    rootFile.mtime = rootFile.atime = getRepository().findById(File.ROOT.id).mtime;
    assertThat(rootFile.equals(getRepository().findById(File.ROOT.id)), is(true));
    assertThat(rootFile.equals(getRepository().findByParentIdAndName(File.ROOT.parentId, File.ROOT.name)), is(true));
    assertThat(getRepository().findByParentId(File.ROOT.id).size() == 0, is(true));
  }
}
