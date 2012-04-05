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

 package org.apache.hadoop.hdfs.server.common;

import java.util.*;

import org.apache.hadoop.conf.Configuration;

public class ConfNameNodeSelector extends AbsNameNodeSelector {
  
	private static Random rand = getRandom();
	
	public ConfNameNodeSelector() {
		super();
		initialize();
		
	}

	private void initialize() {
		namenodeList = new ArrayList<NameNodeInfo>();
		current = -1;
		refreshNameNodeList();
	}

	@Override
	public String refreshNameNodeList() {
	  Configuration conf = new Configuration();
		Collection<String> addresses = conf
				.getStringCollection(DFS_NAMENODE_RPCADDR_LIST);
		List<NameNodeInfo> tmp = new ArrayList<NameNodeInfo>();
		for (String rpc : addresses) {
			NameNodeInfo nni = new NameNodeInfo(rpc);
			tmp.add(nni);
		}

		synchronized (this) {
			if (current != -1) {
				NameNodeInfo nni = namenodeList.get(current);
				if (!tmp.contains(nni)) {
					tmp.add(nni);
					current = tmp.size() - 1;
				} else {
					current = tmp.indexOf(nni);
				}
			}
			namenodeList = tmp;
		}
	
		String ret = new String(tmp.toString());
		tmp = null;

		return ret;
	}
	
	public String refreshNameNodeList(Configuration conf) {
    Collection<String> addresses = conf
        .getStringCollection(DFS_NAMENODE_RPCADDR_LIST);
    List<NameNodeInfo> tmp = new ArrayList<NameNodeInfo>();
    for (String rpc : addresses) {
      NameNodeInfo nni = new NameNodeInfo(rpc);
      tmp.add(nni);
    }

    synchronized (this) {
      if (current != -1) {
        NameNodeInfo nni = namenodeList.get(current);
        if (!tmp.contains(nni)) {
          tmp.add(nni);
          current = tmp.size() - 1;
        } else {
          current = tmp.indexOf(nni);
        }
      }
      namenodeList = tmp;
    }  
    String ret = new String(tmp.toString());
    tmp = null;

    return ret;
  }

	@Override
	public String selectNextNameNodeAddress() {
		NameNodeInfo nni = new NameNodeInfo();
		synchronized (this) {
			if (current != -1) {
				// The current one doesn't work!
				namenodeList.remove(current);
			}
			int size = namenodeList.size();
			if (size == 0) {
				current = -1;
				return null;
			}
			int idx = rand.nextInt(size);
			current = idx;
			nni.setAddress(new String(namenodeList.get(idx).getAddress()));
		}
		return nni.getAddress();

	}

}