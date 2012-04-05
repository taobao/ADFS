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

import junit.framework.TestCase;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;

public class TestMNSetrepIncreasing extends TestCase {
  static void setrep(int fromREP, int toREP, boolean simulatedStorage) throws IOException {
    Configuration conf = new Configuration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    conf.set("dfs.replication", "" + fromREP);
    conf.setLong("dfs.blockreport.intervalMsec", 1000L);
    conf.set("dfs.replication.pending.timeout.sec", Integer.toString(2));
    conf.setStrings("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 10, 0, true, null);
    cluster.waitDatanodeDie();
    FileSystem fs = cluster.getFileSystem(1);
    assertTrue("Not a HDFS: "+fs.getUri(), fs instanceof DistributedFileSystem);

    try {
      Path root = TestDFSShell.mkdir(fs, new Path("/test/setrep" + fromREP + "-" + toREP));
      Path f = TestDFSShell.writeFile(fs, new Path(root, "foo"));
      DFSTestUtil.waitReplication(fs, f, (short)(fromREP));
      
      // Verify setrep for changing replication
      {
        String[] args = {"-setrep", "-w", "" + toREP, "" + f};
        FsShell shell = new FsShell();
        shell.setConf(conf);
        try {
          assertEquals(0, shell.run(args));
        } catch (Exception e) {
          assertTrue("-setrep " + e, false);
        }
      }
      //get fs again since the old one may be closed
      fs = cluster.getFileSystem(1);
      DFSTestUtil.waitReplication(fs, f, (short)(toREP));
      FileStatus file = fs.getFileStatus(f);
      long len = file.getLen();
      for(BlockLocation locations : fs.getFileBlockLocations(file, 0, len)) {
        assertTrue(locations.getHosts().length == toREP);
      }
      TestDFSShell.show("done setrep waiting: " + root);
    } finally {
      try {fs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
  }

  public void testSetrepIncreasing() throws IOException {
    setrep(3, 7, false);
  }
  public void testSetrepIncreasingSimulatedStorage() throws IOException {
    setrep(3, 7, true);
  }
}
