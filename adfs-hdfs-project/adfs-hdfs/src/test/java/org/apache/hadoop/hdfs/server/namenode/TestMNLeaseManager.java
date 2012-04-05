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
package org.apache.hadoop.hdfs.server.namenode;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniMNDFSCluster;

public class TestMNLeaseManager extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestMNLeaseManager.class);

  /*
   * test case: two leases are added for a singler holder, should use
   * the internalReleaseOne method
   */
  //cluster has 2 nn and 3 dn, nn0 has 3 dn, nn1 has 0 dn
  public void testMultiPathLeaseRecovery1()
    throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    URL url = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list","0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 3, 0, true, null);
    multiPathLeaseRecovery(cluster);
    cluster.shutdown();
  }
  
  //cluster has 2 nn and 3 dn, nn0 has 0 dn, nn1 has 3 dn
  public void testMultiPathLeaseRecovery2()
		    throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    URL url = DFSTestUtil.class.getResource("mini-dfs-conf.xml");
    conf.addResource(url);
    conf.set("dfs.namenode.port.list","0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 3, 1, true, null);
    multiPathLeaseRecovery(cluster);
    cluster.shutdown();
  }
  
  private void multiPathLeaseRecovery(MiniMNDFSCluster cluster) throws IOException, InterruptedException{
	NameNode namenode = cluster.getNameNode(0);
    FSNamesystem spyNamesystem = spy(namenode.getNamesystem());
    LeaseManager leaseManager = new LeaseManager(spyNamesystem);
    
    spyNamesystem.leaseManager = leaseManager;
    spyNamesystem.lmthread.interrupt();
    
    String holder = "client-1";
//    String path1 = "/file-1";
//    String path2 = "/file-2";
    int id1 = 1;
    int id2 = 2;
    leaseManager.setLeasePeriod(1, 2);
    leaseManager.addLease(holder, id1);
    leaseManager.addLease(holder, id2);
    Thread.sleep(1000);

    synchronized (spyNamesystem) { // checkLeases is always called with FSN lock
      leaseManager.checkLeases();
    }
    verify(spyNamesystem).internalReleaseLeaseOne((LeaseManager.Lease)anyObject(), eq(id1));
    verify(spyNamesystem).internalReleaseLeaseOne((LeaseManager.Lease)anyObject(), eq(id2));
    verify(spyNamesystem, never()).internalReleaseLease((LeaseManager.Lease)anyObject(), anyInt());
  }
}
