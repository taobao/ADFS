package org.apache.hadoop.tools;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.FSConstants;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ClusterViewer implements Tool, FSConstants{
  
  private static final Log LOG = 
    LogFactory.getLog(ClusterViewer.class.getName());
  
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private Configuration conf;
  private NavigableMap<String, List<String>> nodeMaps = 
    new TreeMap<String, List<String>>();
  private List<String> orphanNodes = new LinkedList<String>();
  private int totalDNs = 0;
  
  @Override
  public int run(String[] args) throws Exception {

    init(args);
    getNamenodes();
    getDatanodes();
    prettyOutput();
   
    return 0;
  }

  private void init(String[] args) throws Exception {
    String zklist = conf.get("zookeeper.server.list");
    if (args.length == 1) {
      conf.set("zookeeper.server.list", args[0]);
      LOG.info("Using specified zklist");
    } else if (args.length == 0 && zklist != null) {
      LOG.info("Using default zklist in conf");
    } else {
      printUsage();
      throw new IllegalArgumentException(Arrays.toString(args));
    }
  }
  
  private void getNamenodes() throws Exception {
    List<String> namenodes = 
      ZKClient.getInstance(conf).getChildren(ZOOKEEPER_NAMENODE_GROUP, null);
    if(namenodes !=null) {
      for(String namenode : namenodes) {
        nodeMaps.put(namenode, new LinkedList<String>());
      }
    }
  }
  
  private void getDatanodes() throws Exception {
    List<String> datanodes = 
      ZKClient.getInstance().getChildren(ZOOKEEPER_DATANODE_GROUNP, null);
    if(datanodes != null) {
      totalDNs = datanodes.size();
      String namenode;
      StringBuilder sb = new StringBuilder();
      for(String datanode : datanodes) {
        sb.delete(0, sb.length());
        sb.append(ZOOKEEPER_DATANODE_GROUNP).append(Path.SEPARATOR).append(datanode);
        namenode = getDatanodeData(sb.toString());
        List<String> val = nodeMaps.get(namenode);
        if(val != null) {
          val.add(datanode);
        } else {
          orphanNodes.add(datanode);
        }
      }
    }
  }
  
  private void prettyOutput() {
    StringBuilder sep = new StringBuilder();
    for(int i = 0; i < 5; i++) {
      sep.append("===");
    }
    sep.append(this.getClass().getSimpleName().toUpperCase());
    long now = System.currentTimeMillis();
    sep.append(String.format("(PRINT TIME: %1$tD,%2$tR)", new Date(now), now));
    for(int i = 0; i < 5; i++) {
      sep.append("===");
    }
    System.out.println(sep.toString());
    System.out.println(
      String.format("Total Namenodes: %1$4d, Total Datanodes: %2$6d", nodeMaps.size()
        , totalDNs));
    System.out.println("Details:");
    java.util.Map.Entry <String, List<String>> entry;
    while((entry=nodeMaps.pollFirstEntry()) != null) {
      System.out.println(String.format("\t Namenode: %1$22s (%2$6d)", entry.getKey(), 
          entry.getValue().size()));
      for(String datanode : entry.getValue()) {
        System.out.println(String.format("\t\t %1$-60s", datanode));
      }
    }
    System.out.println(String.format("\t Orphan Datanodes: (%1$6d)\n\t\t%2$s", 
        orphanNodes.size(), orphanNodes));
    System.out.println(sep.toString());
  }
  
  private String getDatanodeData(String zkpath) throws Exception {
    byte[] data = ZKClient.getInstance().getData(zkpath, null);
    String strData = new String(data, CHARSET);
    return strData.split("/")[0]+ ":" + strData.split(":")[1];
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.conf.addResource("hdfs-site.xml");
  }
  
  private static void printUsage() {
    System.out.println("Usage: java ClusterViewer\t[zookeeperlist]\t");
  }
  
  
  /** the main functions **/
  public static void main(String[] args) {
    try{
      System.exit( ToolRunner.run(null, new ClusterViewer(), args) );
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

}
