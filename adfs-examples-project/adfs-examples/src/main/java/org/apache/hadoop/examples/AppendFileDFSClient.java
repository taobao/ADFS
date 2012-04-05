package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSClient;

public class AppendFileDFSClient {
  
  private static Configuration conf = new Configuration();
  
  private static String make(int length) {
    String radStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    StringBuffer generateRandStr = new StringBuffer();
    Random rand = new Random();
    for (int i = 0; i < length; i++) {
      int randNum = rand.nextInt(36);
      generateRandStr.append(radStr.substring(randNum, randNum + 1));
    }
    return generateRandStr.toString();
  }

  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.out.println("Usage : $0 <count> <file path>");
    }
    conf.addResource(new Path("conf/hdfs-site.xml"));
    int count = Integer.valueOf(args[0]);
    String path = args[1];
    Path p = new Path(path);
    FileSystem fs = p.getFileSystem(conf);
    FSDataOutputStream stm = fs.append(p);
    
    for(int i = 0; i < count; i++) {
      String a = make(1024);
      stm.write(a.getBytes());
      stm.sync();
    }
  }

}
