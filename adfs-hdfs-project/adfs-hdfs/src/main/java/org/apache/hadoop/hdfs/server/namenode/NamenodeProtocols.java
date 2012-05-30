package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;

public interface NamenodeProtocols extends NamenodeProtocol, ClientProtocol, DatanodeProtocol, MonitorProtocol{
  public static final long versionID = 0L;
}
