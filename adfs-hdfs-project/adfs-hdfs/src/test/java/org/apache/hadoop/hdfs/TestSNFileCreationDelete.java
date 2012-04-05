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
package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.log4j.Level;

public class TestSNFileCreationDelete extends junit.framework.TestCase {
  {
    ((Log4JLogger)NameNode.stateChangeLog).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
  }

  public void testFileCreationDeleteParent() throws IOException {
    Configuration conf = new Configuration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt("heartbeat.recheck.interval", 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    conf.setBoolean("dfs.support.append", true);
    conf.setStrings("dfs.namenode.port.list", "0");
    // create cluster
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 1, true, null);
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystemCToFreeNamenode(-1);

      // create file1.
      Path dir = new Path("/foo");
      Path file1 = new Path(dir, "file1");
      FSDataOutputStream stm1 = TestSNFileCreation.createFile(fs, file1, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file1);
      TestSNFileCreation.writeFile(stm1, 1000);
      stm1.sync();

      // create file2.
      Path file2 = new Path("/file2");
      FSDataOutputStream stm2 = TestSNFileCreation.createFile(fs, file2, 1);
      System.out.println("testFileCreationDeleteParent: "
          + "Created file " + file2);
      TestSNFileCreation.writeFile(stm2, 1000);
      stm2.sync();

      // rm dir
      fs.delete(dir, true);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown(false);
      try {Thread.sleep(2*MAX_IDLE_TIME);} catch (InterruptedException e) {}
      cluster = new MiniMNDFSCluster(conf, 1, false, null, false);
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown(false);
      try {Thread.sleep(5000);} catch (InterruptedException e) {}
      cluster = new MiniMNDFSCluster(conf, 1, false, null, false);
      cluster.waitActive();
      fs = cluster.getFileSystemCToFreeNamenode(-1);

      assertTrue(!fs.exists(file1));
      assertTrue(fs.exists(file2));
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }
}
