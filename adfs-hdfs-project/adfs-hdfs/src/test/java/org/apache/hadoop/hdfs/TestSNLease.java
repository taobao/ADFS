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

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.DbNodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

public class TestSNLease extends junit.framework.TestCase {
  static int fs_nnindex = 0;
  static boolean hasLease(MiniMNDFSCluster cluster, Path src) throws IOException {
	  DbNodeInfo dbNode = cluster.getNameNode(fs_nnindex).namesystem.fileManager.getDbNodeInfo(src.toString());
	  if(dbNode!=null)
	  {
		  int fid = dbNode.fid;
		  return cluster.getNameNode(fs_nnindex).namesystem.leaseManager.getLeaseById(fid) != null;
	  }
	  else 
		  return false;
  }
  
  final Path dir = new Path("/test/lease/");

  public void testLease() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 2, true, null);
    cluster.waitActive();
    try {
      FileSystem fs = cluster.getFileSystem(fs_nnindex);
      assertTrue(fs.mkdirs(dir));
      
      Path a = new Path(dir, "a");
      Path b = new Path(dir, "b");

      DataOutputStream a_out = fs.create(a);
      a_out.writeBytes("something");
      
      assertTrue(hasLease(cluster, a));
      assertTrue(!hasLease(cluster, b));
      
      DataOutputStream b_out = fs.create(b);
      b_out.writeBytes("something");

      assertTrue(hasLease(cluster, a));
      assertTrue(hasLease(cluster, b));

      a_out.close();
      b_out.close();

      assertTrue(!hasLease(cluster, a));
      assertTrue(!hasLease(cluster, b));
      
      fs.delete(dir, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}
