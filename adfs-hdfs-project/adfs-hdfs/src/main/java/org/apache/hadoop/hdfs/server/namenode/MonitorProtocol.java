package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

public interface MonitorProtocol {
  public static final long versionID = 2L;

  public void heartbeatCheck() throws IOException;

  public void replicationCheck() throws IOException;

  public void pendingReplicationCheck() throws IOException;

  public void decommisionCheck() throws IOException;
}
