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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;

public abstract class AbsNameNodeSelector implements FSConstants{
	
	protected volatile int current; 
	
	protected List<NameNodeInfo> namenodeList;

	public abstract String selectNextNameNodeAddress();

	public abstract String refreshNameNodeList();
	public abstract String refreshNameNodeList(Configuration conf);

  private static int getPid() {
    int id = -1;
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    String name = runtime.getName(); // format: "pid@hostname"
    try {
        id = Integer.parseInt(name.substring(0, name.indexOf('@')));
    } catch (Exception ingored) {}
    return id;
  }
  
  public static Random getRandom() {
    return new Random(System.nanoTime()+getPid());
  }
  
	/**
	 * 
	 * NameNodeInfo represents some information of a Name Node. Currently, it is
	 * used by NameNodeSelector for choosing one Name Node for a Data Node.
	 * 
	 */
	class NameNodeInfo {

		/**
		 *  RPC Address of a Name Node
		 */
		private String address;

		public NameNodeInfo() {

		}

		public NameNodeInfo(String addr) {
			address = addr;
		}

		public String getAddress() {
			return address;
		}

		public void setAddress(String address) {
			this.address = address;
		}

		@Override
		public String toString() {
			return "<" + address + ">";
		}

		@Override
		public int hashCode() {
			return address.hashCode();
		}

		@Override
		public boolean equals(Object obj) {

			if (obj == null) {
				return false;
			}

			if (this == obj) {
				return true;
			}

			if (obj instanceof NameNodeInfo) {
				return address.equals(((NameNodeInfo) obj).getAddress());
			}

			return false;
		}

	}

}
