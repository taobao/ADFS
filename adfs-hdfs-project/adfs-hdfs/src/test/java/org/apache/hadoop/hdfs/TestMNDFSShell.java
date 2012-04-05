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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class tests commands from DFSShell.
 */
public class TestMNDFSShell extends TestCase {
  static final String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","build/test/data"))
    .toString().replace(' ', '+');

  static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  static Path mkdir(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.exists(p));
    assertTrue(fs.getFileStatus(p).isDir());
    return p;
  }

  static File createLocalFile(File f) throws IOException {
    assertTrue(!f.exists());
    PrintWriter out = new PrintWriter(f);
    out.print("createLocalFile: " + f.getAbsolutePath());
    out.flush();
    out.close();
    assertTrue(f.exists());
    assertTrue(f.isFile());
    return f;
  }

  static void show(String s) {
    System.out.println(Thread.currentThread().getStackTrace()[2] + " " + s);
  }

  public void testZeroSizeFile() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    File f1 = null;
    File f2 = null;

    try {
      //create a zero size file
      f1 = new File(TEST_ROOT_DIR, "f1");
      assertTrue(!f1.exists());
      assertTrue(f1.createNewFile());
      assertTrue(f1.exists());
      assertTrue(f1.isFile());
      assertEquals(0L, f1.length());
      
      //copy to remote
      final Path root = mkdir(dfs, new Path("/test/zeroSizeFile"));
      final Path remotef = new Path(root, "dst");
      show("copy local " + f1 + " to remote " + remotef);
      dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), remotef);
      
      //getBlockSize() should not throw exception
      show("Block size = " + dfs.getFileStatus(remotef).getBlockSize());

      //copy back
      f2 = new File(TEST_ROOT_DIR, "f2");
      assertTrue(!f2.exists());
      dfs.copyToLocalFile(remotef, new Path(f2.getPath()));
      assertTrue(f2.exists());
      assertTrue(f2.isFile());
      assertEquals(0L, f2.length());
    } finally {
      testAndDeleteFile(f1);
      testAndDeleteFile(f2);
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
    }
  }
  
  public void testRecrusiveRm() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: " + fs.getUri(), 
        fs instanceof DistributedFileSystem);
    try {
      fs.mkdirs(new Path(new Path("parent"), "child"));
      try {
        fs.delete(new Path("parent"), false);
        fail(); // should never reach here.
      } catch(IOException e) {
         //should have thrown an exception
      }
      try {
        fs.delete(new Path("parent"), true);
      } catch(IOException e) {
        fail();
      }
    } finally {  
      try { fs.close();}catch(IOException e){};
      cluster.shutdown();
    }
  }
    
  /**
   * Two files are written from diff NNs, and files should be available 
   * to all NNs, inside the du function. 
   * @throws IOException
   */
  public void testDu() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs0 = cluster.getFileSystem(0);
    FileSystem fs1 = cluster.getFileSystem(1);
    assertTrue("Not a HDFS: "+fs0.getUri(),
                fs0 instanceof DistributedFileSystem);
    assertTrue("Not a HDFS: "+fs1.getUri(),
        fs1 instanceof DistributedFileSystem);
    final DistributedFileSystem dfs0 = (DistributedFileSystem)fs0;
    final DistributedFileSystem dfs1 = (DistributedFileSystem)fs1;
    PrintStream psBackup = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream psOut = new PrintStream(out);
    System.setOut(psOut);
    FsShell shell = new FsShell();
    shell.setConf(conf);
    
    try {
      Path myPath = new Path("/test/dir");
      assertTrue(fs0.mkdirs(myPath));
      assertTrue(fs1.exists(myPath));
      Path myFile = new Path("/test/dir/file");
      writeFile(fs0, myFile);
      assertTrue(fs1.exists(myFile));
      Path myFile2 = new Path("/test/dir/file2");
      writeFile(fs1, myFile2);
      assertTrue(fs0.exists(myFile2));
      
      String[] args = new String[2];
      args[0] = "-du";
      args[1] = "/test/dir";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      String returnString = out.toString();
      out.reset();
      // Check if size matchs as expected
      assertTrue(returnString.contains("22"));
      assertTrue(returnString.contains("23"));
      
    } finally {
      try {dfs0.close();} catch (Exception e) {}
      try {dfs1.close();} catch (Exception e) {}
      System.setOut(psBackup);
      cluster.shutdown();
    }
                                  
  }
  public void testPut() throws IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    final DistributedFileSystem dfs = (DistributedFileSystem)fs;
    final File f1 = createLocalFile(new File(TEST_ROOT_DIR, "f1"));
    final File f2 = createLocalFile(new File(TEST_ROOT_DIR, "f2"));

    try {
      // remove left over crc files:
      new File(TEST_ROOT_DIR, ".f1.crc").delete();
      new File(TEST_ROOT_DIR, ".f2.crc").delete();    
  
      final Path root = mkdir(dfs, new Path("/test/put"));
      final Path dst = new Path(root, "dst");
  
      show("begin");
      
      final Thread copy2ndFileThread = new Thread() {
        public void run() {
          try {
            show("copy local " + f2 + " to remote " + dst);
            dfs.copyFromLocalFile(false, false, new Path(f2.getPath()), dst);
          } catch (IOException ioe) {
            show("good " + StringUtils.stringifyException(ioe));
            return;
          }
          //should not be here, must got IOException
          assertTrue(false);
        }
      };
      
      //use SecurityManager to pause the copying of f1 and begin copying f2
      SecurityManager sm = System.getSecurityManager();
      System.out.println("SecurityManager = " + sm);
      System.setSecurityManager(new SecurityManager() {
        private boolean firstTime = true;
  
        public void checkPermission(Permission perm) {
          if (firstTime) {
            Thread t = Thread.currentThread();
            if (!t.toString().contains("DataNode")) {
              String s = "" + Arrays.asList(t.getStackTrace());
              if (s.contains("FileUtil.copyContent")) {
                //pause at FileUtil.copyContent
  
                firstTime = false;
                copy2ndFileThread.start();
                try {Thread.sleep(5000);} catch (InterruptedException e) {}
              }
            }
          }
        }
      });
      show("copy local " + f1 + " to remote " + dst);
      dfs.copyFromLocalFile(false, false, new Path(f1.getPath()), dst);
      show("done");
  
      try {copy2ndFileThread.join();} catch (InterruptedException e) { }
      System.setSecurityManager(sm);

      // copy multiple files to destination directory
      final Path destmultiple = mkdir(dfs, new Path("/test/putmultiple"));
      Path[] srcs = new Path[2];
      srcs[0] = new Path(f1.getPath());
      srcs[1] = new Path(f2.getPath());
      dfs.copyFromLocalFile(false, false, srcs, destmultiple);
      srcs[0] = new Path(destmultiple,"f1"); 
      srcs[1] = new Path(destmultiple,"f2"); 
      assertTrue(dfs.exists(srcs[0]));
      assertTrue(dfs.exists(srcs[1]));

      // move multiple files to destination directory
      final Path destmultiple2 = mkdir(dfs, new Path("/test/movemultiple"));
      srcs[0] = new Path(f1.getPath());
      srcs[1] = new Path(f2.getPath());
      dfs.moveFromLocalFile(srcs, destmultiple2);
      assertFalse(f1.exists());
      assertFalse(f2.exists());
      srcs[0] = new Path(destmultiple2, "f1");
      srcs[1] = new Path(destmultiple2, "f2");
      assertTrue(dfs.exists(srcs[0]));
      assertTrue(dfs.exists(srcs[1]));

    } finally {
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
      testAndDeleteFile(f1);
      testAndDeleteFile(f2);
    }
  }


  /** check command error outputs and exit statuses. */
  public void testErrOutPut() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = null;
    PrintStream bak = null;
    FileSystem srcFs = null; 
    try {
      cluster = new MiniMNDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      cluster.waitDatanodeDie();
      srcFs = cluster.getFileSystem();
      Path root = new Path("/nonexistentfile");
      bak = System.err;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      PrintStream tmp = new PrintStream(out);
      System.setErr(tmp);
      String[] argv = new String[2];
      argv[0] = "-cat";
      argv[1] = root.toUri().getPath();
      int ret = ToolRunner.run(new FsShell(conf), argv);
      assertTrue(" -cat returned -1 ", 0>ret);
      String returned = out.toString();
      assertTrue("cat does not print exceptions ",
          (returned.lastIndexOf("Exception") == -1));
      out.reset();
      argv[0] = "-rm";
      argv[1] = root.toString();
      FsShell shell = new FsShell();
      shell.setConf(conf);
      ret = ToolRunner.run(shell, argv);
      assertTrue(" -rm returned -1 ", 0>ret);
      returned = out.toString();
      out.reset();
      assertTrue("rm prints reasonable error ",
          (returned.lastIndexOf("No such file or directory") != -1));
      argv[0] = "-rmr";
      argv[1] = root.toString();
      ret = ToolRunner.run(shell, argv);
      assertTrue(" -rmr returned -1", 0>ret);
      returned = out.toString();
      assertTrue("rmr prints reasonable error ",
          (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-du";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -du prints reasonable error ",
          (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-dus";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -dus prints reasonable error",
          (returned.lastIndexOf("No such file or directory") != -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/nonexistenfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -ls does not return Found 0 items",
          (returned.lastIndexOf("Found 0") == -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/nonexistentfile";
      ret = ToolRunner.run(shell, argv);
      assertTrue(" -lsr should fail ",
          (ret < 0));
      out.reset();
      srcFs.mkdirs(new Path("/testdir"));
      argv[0] = "-ls";
      argv[1] = "/testdir";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -ls does not print out anything ",
          (returned.lastIndexOf("Found 0") == -1));
      out.reset();
      argv[0] = "-ls";
      argv[1] = "/user/nonxistant/*";
      ret = ToolRunner.run(shell, argv);
      assertTrue(" -ls on nonexistent glob returns -1",
          (ret < 0));
      out.reset();
      argv[0] = "-mkdir";
      argv[1] = "/testdir";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -mkdir returned -1 ", (ret < 0));
      assertTrue(" -mkdir returned File exists", 
          (returned.lastIndexOf("File exists") != -1));
      Path testFile = new Path("/testfile");
      OutputStream outtmp = srcFs.create(testFile);
      outtmp.write(testFile.toString().getBytes());
      outtmp.close();
      out.reset();
      argv[0] = "-mkdir";
      argv[1] = "/testfile";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" -mkdir returned -1", (ret < 0));
      assertTrue(" -mkdir returned this is a file ",
          (returned.lastIndexOf("not a directory") != -1));
      out.reset();
      argv = new String[3];
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "file";
      ret = ToolRunner.run(shell, argv);
      assertTrue("mv failed to rename", ret == 0);
      out.reset();
      argv = new String[3];
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "/testfiletest";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue("no output from rename", 
          (returned.lastIndexOf("Renamed") == -1));
      out.reset();
      argv[0] = "-mv";
      argv[1] = "/testfile";
      argv[2] = "/testfiletmp";
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" unix like output",
          (returned.lastIndexOf("No such file or") != -1));
      out.reset();
      argv = new String[1];
      argv[0] = "-du";
      srcFs.mkdirs(srcFs.getHomeDirectory());
      ret = ToolRunner.run(shell, argv);
      returned = out.toString();
      assertTrue(" no error ", (ret == 0));
      assertTrue("empty path specified",
          (returned.lastIndexOf("empty string") == -1));
    } finally {
      if (srcFs != null)
        srcFs.close();
      if (bak != null) {
        System.setErr(bak);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  

  public void testURIPaths() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = null;
    File furi = null;
    try{
      cluster = new MiniMNDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      cluster.waitDatanodeDie();
      FileSystem fs = cluster.getFileSystem();
      FsShell shell = new FsShell();
      shell.setConf(conf);
      //check for ls
      String[] argv = new String[2];
      argv[0] = "-ls";
      argv[1] = fs.getUri().toString() + "/";
      int ret = ToolRunner.run(shell, argv);
      assertTrue("ls works on remote uri ", (ret==0));
      //check for rm -r 
      fs.mkdirs(new Path("/hadoopdir"));
      argv = new String[2];
      argv[0] = "-rmr";
      argv[1] = fs.getUri().toString() + "/hadoopdir";
      ret = ToolRunner.run(shell, argv);
      assertTrue("-rmr works on remote uri " + argv[1], (ret==0));
      //check du 
      argv[0] = "-du";
      argv[1] = fs.getUri().toString() + "/";
      ret = ToolRunner.run(shell, argv);
      assertTrue("du works on remote uri ", (ret ==0));
      //check put
      furi = new File(TEST_ROOT_DIR, "furi");
      createLocalFile(furi);
      argv = new String[3];
      argv[0] = "-put";
      argv[1] = furi.toString();
      argv[2] = fs.getUri().toString() + "/furi";
      ret = ToolRunner.run(shell, argv);
      assertTrue(" put is working ", (ret==0));
      //check cat 
      argv = new String[2];
      argv[0] = "-cat";
      argv[1] = fs.getUri().toString() + "/furi";
      ret = ToolRunner.run(shell, argv);
      assertTrue(" cat is working ", (ret == 0));
      //check chown
      //dstFs.delete(new Path("/furi"), true);
      fs.delete(new Path("/hadoopdir"), true);
      String file = "/tmp/chownTest";
      Path path = new Path(file);
      Path parent = new Path("/tmp");
      Path root = new Path("/");
      TestMNDFSShell.writeFile(fs, path);
      runCmd(shell, "-chgrp", "-R", "herbivores", fs.getUri().toString() +"/*");
      confirmOwner(null, "herbivores", fs, parent, path);
      runCmd(shell, "-chown", "-R", ":reptiles", fs.getUri().toString() + "/");
      confirmOwner(null, "reptiles", fs, root, parent, path);
      //check if default hdfs:/// works 
      argv[0] = "-cat";
      argv[1] = "hdfs:///furi";
      ret = ToolRunner.run(shell, argv);
      assertTrue(" default works for cat", (ret == 0));
      argv[0] = "-ls";
      argv[1] = "hdfs:///";
      ret = ToolRunner.run(shell, argv);
      assertTrue("default works for ls ", (ret == 0));
      argv[0] = "-rmr";
      argv[1] = "hdfs:///furi";
      ret = ToolRunner.run(shell, argv);
      assertTrue("default works for rm/rmr", (ret ==0));
    } finally {
      testAndDeleteFile(furi);
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }

  public void testText() throws Exception {
    Configuration conf = new Configuration();
    MiniMNDFSCluster cluster = null;
    PrintStream bak = null;
    FileSystem fs = null;
    try {
      conf.set("dfs.namenode.port.list", "0,0");
      cluster = new MiniMNDFSCluster(conf, 4, true, null);
      cluster.waitActive();
      cluster.waitDatanodeDie();
      fs = cluster.getFileSystem();
      Path root = new Path("/texttest");
      fs.mkdirs(root);
      OutputStream zout = new GZIPOutputStream(
          fs.create(new Path(root, "file.gz")));
      Random r = new Random();
      ByteArrayOutputStream file = new ByteArrayOutputStream();
      for (int i = 0; i < 1024; ++i) {
        char c = Character.forDigit(r.nextInt(26) + 10, 36);
        file.write(c);
        zout.write(c);
      }
      zout.close();

      bak = System.out;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      System.setOut(new PrintStream(out));

      String[] argv = new String[2];
      argv[0] = "-text";
      argv[1] = new Path(root, "file.gz").toUri().getPath();
      int ret = ToolRunner.run(new FsShell(conf), argv);
      assertTrue("-text returned -1", 0 == ret);
//      file.reset();
//      out.reset();
      assertTrue("Output doesn't match input",
          Arrays.equals(file.toByteArray(), out.toByteArray()));

    } finally {
      if (null != bak) {
        System.setOut(bak);
      }
      if (fs != null)
        fs.close();
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }

  public void testCopyToLocal() throws IOException {
    Configuration conf = new Configuration();
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.hdfs.ChecksumDistributedFileSystem");
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs = cluster.getFileSystem();
    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof ChecksumDistributedFileSystem);
    ChecksumDistributedFileSystem dfs = (ChecksumDistributedFileSystem)fs;
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      String root = createTree(dfs, "copyToLocal");

      // Verify copying the tree
      {
        try {
          assertEquals(0,
              runCmd(shell, "-copyToLocal", root + "*", TEST_ROOT_DIR));
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }

        File localroot = new File(TEST_ROOT_DIR, "copyToLocal");
        File localroot2 = new File(TEST_ROOT_DIR, "copyToLocal2");        
        
        File f1 = null;
        File f2 = null;
        File sub = null;
        File f3 = null;
        File f4 = null;
        File f5 = null;
        try {
          f1 = new File(localroot, "f1");
          assertTrue("Copying failed.", f1.isFile());

          f2 = new File(localroot, "f2");
          assertTrue("Copying failed.", f2.isFile());

          sub = new File(localroot, "sub");
          assertTrue("Copying failed.", sub.isDirectory());

          f3 = new File(sub, "f3");
          assertTrue("Copying failed.", f3.isFile());

          f4 = new File(sub, "f4");
          assertTrue("Copying failed.", f4.isFile());
          
          f5 = new File(localroot2, "f1");
          assertTrue("Copying failed.", f5.isFile());
        } finally {
          testAndDeleteFile(f1);
          testAndDeleteFile(f2);
          testAndDeleteFile(f3);
          testAndDeleteFile(f4);
          testAndDeleteFile(f5);
          testAndDeleteFile(sub);
        }
      }
      // Verify copying non existing sources do not create zero byte
      // destination files
      {
        String[] args = {"-copyToLocal", "nosuchfile", TEST_ROOT_DIR};
        try {   
          assertEquals(-1, shell.run(args));
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                            e.getLocalizedMessage());
        }                            
        File f6 = new File(TEST_ROOT_DIR, "nosuchfile");
        assertTrue(!f6.exists());
      }
    } finally {
      try {
        dfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }

  static String createTree(FileSystem fs, String name) throws IOException {
    // create a tree
    //   ROOT
    //   |- f1
    //   |- f2
    //   + sub
    //      |- f3
    //      |- f4
    //   ROOT2
    //   |- f1
    String path = "/test/" + name;
    Path root = mkdir(fs, new Path(path));
    Path sub = mkdir(fs, new Path(root, "sub"));
    Path root2 = mkdir(fs, new Path(path + "2"));        

    writeFile(fs, new Path(root, "f1"));
    writeFile(fs, new Path(root, "f2"));
    writeFile(fs, new Path(sub, "f3"));
    writeFile(fs, new Path(sub, "f4"));
    writeFile(fs, new Path(root2, "f1"));
    mkdir(fs, new Path(root2, "sub"));
    return path;
  }

  /*public void testCount() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      String root = createTree(dfs, "count");

      // Verify the counts
      runCount(root, 2, 4, conf);
      runCount(root + "2", 2, 1, conf);
      runCount(root + "2/f1", 0, 1, conf);
      runCount(root + "2/sub", 1, 0, conf);

      final FileSystem localfs = FileSystem.getLocal(conf);
      Path localpath = new Path(TEST_ROOT_DIR, "testcount");
      localpath = localpath.makeQualified(localfs);
      localfs.mkdirs(localpath);
      
      final String localstr = localpath.toString();
      System.out.println("localstr=" + localstr);
      runCount(localstr, 1, 0, conf);
      assertEquals(0, new Count(new String[]{root, localstr}, 0, conf).runAll());
    } finally {
      try {
        dfs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }*/
  private void runCount(String path, long dirs, long files, Configuration conf
    ) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    PrintStream out = new PrintStream(bytes);
    PrintStream oldOut = System.out;
    System.setOut(out);
    Scanner in = null;
    String results = null;
    try {
      new Count(new String[]{path}, 0, conf).runAll();
      results = bytes.toString();
      in = new Scanner(results);
      assertEquals(dirs, in.nextLong());
      assertEquals(files, in.nextLong());
    } finally {
      if (in!=null) in.close();
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.out.println("results:\n" + results);
    }
  }

  //throws IOException instead of Exception as shell.run() does.
  private int runCmd(FsShell shell, String... args) throws IOException {
    try {
      return shell.run(args);
    } catch (IOException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
  }
  
  /**
   * Test chmod.
   */
  void testChmod(Configuration conf, FileSystem fs, String chmodDir) 
                                                    throws IOException {
    FsShell shell = new FsShell();
    shell.setConf(conf);
    
    try {
     //first make dir
     Path dir = new Path(chmodDir);
     fs.delete(dir, true);
     fs.mkdirs(dir);

     runCmd(shell, "-chmod", "u+rwx,g=rw,o-rwx", chmodDir);
     assertEquals("rwxrw----",
                  fs.getFileStatus(dir).getPermission().toString());

     //create an empty file
     Path file = new Path(chmodDir, "file");
     TestMNDFSShell.writeFile(fs, file);

     //test octal mode
     runCmd(shell, "-chmod", "644", file.toString());
     assertEquals("rw-r--r--",
                  fs.getFileStatus(file).getPermission().toString());

     //test recursive
     runCmd(shell, "-chmod", "-R", "a+rwX", chmodDir);
     assertEquals("rwxrwxrwx",
                  fs.getFileStatus(dir).getPermission().toString()); 
     assertEquals("rw-rw-rw-",
                  fs.getFileStatus(file).getPermission().toString());
     
     fs.delete(dir, true);     
    } finally {
      try {
        fs.close();
        shell.close();
      } catch (IOException ignored) {}
    }
  }
  
  private void confirmOwner(String owner, String group, 
                            FileSystem fs, Path... paths) throws IOException {
    for(Path path : paths) {
      if (owner != null) {
        assertEquals(owner, fs.getFileStatus(path).getOwner());
      }
      if (group != null) {
        //assertEquals(group, fs.getFileStatus(path).getGroup());
      }
    }
  }
  
  /*public void testFilePermissions() throws IOException {
    Configuration conf = new Configuration();
    
    //test chmod on local fs
    FileSystem fs = FileSystem.getLocal(conf);
    testChmod(conf, fs, 
              (new File(TEST_ROOT_DIR, "chmodTest")).getAbsolutePath());
    
    conf.set("dfs.permissions", "true");
    
    //test chmod on DFS
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    fs = cluster.getFileSystem();
    testChmod(conf, fs, "/tmp/chmodTest");
    
    // test chown and chgrp on DFS:
    
    FsShell shell = new FsShell();
    shell.setConf(conf);
    fs = cluster.getFileSystem();
    
     For dfs, I am the super user and I can change ower of any file to
     * anything. "-R" option is already tested by chmod test above.
     
    
    String file = "/tmp/chownTest";
    Path path = new Path(file);
    Path parent = new Path("/tmp");
    Path root = new Path("/");
    TestMNDFSShell.writeFile(fs, path);
    
    runCmd(shell, "-chgrp", "-R", "herbivores", "/*", "unknownFile*");
    confirmOwner(null, "herbivores", fs, parent, path);
    
    runCmd(shell, "-chgrp", "mammals", file);
    confirmOwner(null, "mammals", fs, path);
    
    runCmd(shell, "-chown", "-R", ":reptiles", "/");
    confirmOwner(null, "reptiles", fs, root, parent, path);
    
    runCmd(shell, "-chown", "python:", "/nonExistentFile", file);
    confirmOwner("python", "reptiles", fs, path);

    runCmd(shell, "-chown", "-R", "hadoop:toys", "unknownFile", "/");
    confirmOwner("hadoop", "toys", fs, root, parent, path);
    
    // Test different characters in names

    runCmd(shell, "-chown", "hdfs.user", file);
    confirmOwner("hdfs.user", null, fs, path);
    
    runCmd(shell, "-chown", "_Hdfs.User-10:_hadoop.users--", file);
    confirmOwner("_Hdfs.User-10", "_hadoop.users--", fs, path);
    
    runCmd(shell, "-chown", "hdfs/hadoop-core@apache.org:asf-projects", file);
    confirmOwner("hdfs/hadoop-core@apache.org", "asf-projects", fs, path);
    
    runCmd(shell, "-chgrp", "hadoop-core@apache.org/100", file);
    confirmOwner(null, "hadoop-core@apache.org/100", fs, path);
    
    cluster.shutdown();
  }*/
  /**
   * Tests various options of DFSShell.
   * Operations from NN 0 should be visible from NN 1. 
   * TODO: diff scenarios...
   */
  public void TestMNDFSShell() throws IOException {
    Configuration conf = new Configuration();
    /* This tests some properties of ChecksumFileSystem as well.
     * Make sure that we create ChecksumDFS */
    conf.set("fs.hdfs.impl",
             "org.apache.hadoop.hdfs.ChecksumDistributedFileSystem");
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    FileSystem fs0 = cluster.getFileSystem(0);
    FileSystem fs1 = cluster.getFileSystem(1);
    assertTrue("Not a HDFS: "+fs0.getUri(),
            fs0 instanceof ChecksumDistributedFileSystem);
    assertTrue("Not a HDFS: "+fs1.getUri(),
        fs1 instanceof ChecksumDistributedFileSystem);
    ChecksumDistributedFileSystem fileSys0 = (ChecksumDistributedFileSystem)fs0;
    ChecksumDistributedFileSystem fileSys1 = (ChecksumDistributedFileSystem)fs1;
    FsShell shell = new FsShell();
    shell.setConf(conf);

    try {
      // First create a new directory with mkdirs
      Path myPath = new Path("/test/mkdirs");
      assertTrue(fileSys0.mkdirs(myPath));
      assertTrue(fileSys0.exists(myPath));
      assertTrue(fileSys1.exists(myPath));
      assertTrue(fileSys1.mkdirs(myPath));

      // Second, create a file in that directory.
      Path myFile = new Path("/test/mkdirs/myFile");
      writeFile(fileSys0, myFile);
      // this creation should be visible from FileSystem 1.
      assertTrue(fileSys1.exists(myFile));
      Path myFile2 = new Path("/test/mkdirs/myFile2");      
      writeFile(fileSys0, myFile2);
      assertTrue(fileSys1.exists(myFile2));

      // Verify that rm with a pattern
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile*";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
        assertFalse(fileSys1.exists(myFile));
        assertFalse(fileSys1.exists(myFile2));

        //re-create the files for other tests
        writeFile(fileSys0, myFile);
        assertTrue(fileSys1.exists(myFile));
        writeFile(fileSys0, myFile2);
        assertTrue(fileSys1.exists(myFile2));
      }

      // Verify that we can read the file
      {
        String[] args = new String[3];
        args[0] = "-cat";
        args[1] = "/test/mkdirs/myFile";
        args[2] = "/test/mkdirs/myFile2";        
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run: " +
                             StringUtils.stringifyException(e)); 
        }
        assertTrue(val == 0);
      }
      fileSys1.delete(myFile2, true);

      // Verify that we can get with and without crc
      {
        File testFile = new File(TEST_ROOT_DIR, "mkdirs/myFile");
        File checksumFile = new File(fileSys0.getChecksumFile(
                                     new Path(testFile.getAbsolutePath())).toString());
        testFile.delete();
        checksumFile.delete();
          
        try {
          String[] args = new String[3];
          args[0] = "-get";
          args[1] = "/test/mkdirs";
          args[2] = TEST_ROOT_DIR;
          int val = -1;
          try {
            val = shell.run(args);
          } catch (Exception e) {
            System.err.println("Exception raised from DFSShell.run " +
                               e.getLocalizedMessage()); 
          }
          assertTrue(val == 0);
          assertTrue("Copying failed.", testFile.exists());
          assertTrue("Checksum file " + checksumFile+" is copied.", !checksumFile.exists());
        } finally {
          testAndDeleteFile(testFile);
        }
      }
      {
        File testFile = new File(TEST_ROOT_DIR, "mkdirs/myFile");
        File checksumFile = new File(fileSys0.getChecksumFile(
                                     new Path(testFile.getAbsolutePath())).toString());
        testFile.delete();
        checksumFile.delete();
          
        try {
          String[] args = new String[4];
          args[0] = "-get";
          args[1] = "-crc";
          args[2] = "/test/mkdirs";
          args[3] = TEST_ROOT_DIR;
          int val = -1;
          try {
            val = shell.run(args);
          } catch (Exception e) {
            System.err.println("Exception raised from DFSShell.run " +
                               e.getLocalizedMessage()); 
          }
          assertTrue(val == 0);
            
          assertTrue("Copying data file failed.", testFile.exists());
          assertTrue("Checksum file " + checksumFile+" not copied.", checksumFile.exists());
        } finally {
          testAndDeleteFile(testFile);
          testAndDeleteFile(checksumFile);
        }
      }
      // Verify that we get an error while trying to read an nonexistent file
      {
        String[] args = new String[2];
        args[0] = "-cat";
        args[1] = "/test/mkdirs/myFile1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val != 0);
      }

      // Verify that we get an error while trying to delete an nonexistent file
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val != 0);
      }

      // Verify that we succeed in removing the file we created
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // Verify touch/test
      {
        String[] args = new String[2];
        args[0] = "-touchz";
        args[1] = "/test/mkdirs/noFileHere";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);

        args = new String[3];
        args[0] = "-test";
        args[1] = "-e";
        args[2] = "/test/mkdirs/noFileHere";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);
      }

      // Verify that cp from a directory to a subdirectory fails
      {
        String[] args = new String[2];
        args[0] = "-mkdir";
        args[1] = "/test/dir1";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);

        // this should fail
        String[] args1 = new String[3];
        args1[0] = "-cp";
        args1[1] = "/test/dir1";
        args1[2] = "/test/dir1/dir2";
        val = 0;
        try {
          val = shell.run(args1);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == -1);

        // this should succeed
        args1[0] = "-cp";
        args1[1] = "/test/dir1";
        args1[2] = "/test/dir1foo";
        val = -1;
        try {
          val = shell.run(args1);
        } catch (Exception e) {
          System.err.println("Exception raised from DFSShell.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(val == 0);
      }
        
    } finally {
      try {
        fileSys0.close();
        fileSys1.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }

  static List<File> getBlockFiles(MiniMNDFSCluster cluster) throws IOException {
    List<File> files = new ArrayList<File>();
    List<DataNode> datanodes = cluster.getDataNodes();
    Block[][] blocks = cluster.getAllBlockReports();
    for(int i = 0; i < blocks.length; i++) {
      FSDataset ds = (FSDataset)datanodes.get(i).getFSDataset();
      for(Block b : blocks[i]) {
        files.add(ds.getBlockFile(b));
      }        
    }
    return files;
  }

  static void corrupt(List<File> files) throws IOException {
    for(File f : files) {
      StringBuilder content = new StringBuilder(DFSTestUtil.readFile(f));
      char c = content.charAt(0);
      content.setCharAt(0, ++c);
      PrintWriter out = new PrintWriter(f);
      out.print(content);
      out.flush();
      out.close();      
    }
  }

  static interface TestGetRunner {
    String run(int exitcode, String... options) throws IOException;
  }

  public void testRemoteException() throws Exception {
    UnixUserGroupInformation tmpUGI = new UnixUserGroupInformation("tmpname",
        new String[] {
        "mygroup"});
    MiniMNDFSCluster cluster = null;
    PrintStream bak = null;
    FileSystem fs = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniMNDFSCluster(conf, 2, true, null);
      cluster.waitActive();
      cluster.waitDatanodeDie();
      fs = cluster.getFileSystem();
      Path p = new Path("/foo");
      fs.mkdirs(p);
      fs.setPermission(p, new FsPermission((short)0700));
      UnixUserGroupInformation.saveToConf(conf,
          UnixUserGroupInformation.UGI_PROPERTY_NAME, tmpUGI);
      FsShell fshell = new FsShell(conf);
      bak = System.err;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      PrintStream tmp = new PrintStream(out);
      System.setErr(tmp);
      String[] args = new String[2];
      args[0] = "-ls";
      args[1] = "/foo";
      int ret = ToolRunner.run(fshell, args);
      assertTrue("returned should be 0", (ret == 0));
      //TODO: permissions not implemented yet.
      //String str = out.toString();
      //assertTrue("permission denied printed", str.indexOf("Permission denied") != -1);
      out.reset();
    } finally {
      if (bak != null) {
        System.setErr(bak);
      }
      if (fs != null)
        fs.close();
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  public void testGet() throws IOException {
    DFSTestUtil.setLogLevel2All(FSInputChecker.LOG);
    final Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
    final String fname = "testGet.txt";
    final File localf = createLocalFile(new File(TEST_ROOT_DIR, fname));

    try {
      final String localfcontent = DFSTestUtil.readFile(localf);
      final Path root = mkdir(dfs, new Path("/test/get"));
      final Path remotef = new Path(root, fname);
      dfs.copyFromLocalFile(false, false, new Path(localf.getPath()), remotef);

      final FsShell shell = new FsShell();
      shell.setConf(conf);
      TestGetRunner runner = new TestGetRunner() {
        private int count = 0;

        public String run(int exitcode, String... options) throws IOException {
          File dFile = null;
          try {
            String dst = TEST_ROOT_DIR + "/" + fname+ ++count;
            String[] args = new String[options.length + 3];
            args[0] = "-get"; 
            args[args.length - 2] = remotef.toString();
            args[args.length - 1] = dst;
            for(int i = 0; i < options.length; i++) {
              args[i + 1] = options[i];
            }
            show("args=" + Arrays.asList(args));
            
            try {
              assertEquals(exitcode, shell.run(args));
            } catch (Exception e) {
              assertTrue(StringUtils.stringifyException(e), false); 
            }
            String ret = null;
            dFile = new File(dst);
            if (exitcode == 0)
              ret = DFSTestUtil.readFile(dFile);
            return ret;
          } finally {
            testAndDeleteFile(dFile);
          }
        }
      };

      assertEquals(localfcontent, runner.run(0));
      assertEquals(localfcontent, runner.run(0, "-ignoreCrc"));

      //find and modify the block files
      List<File> files = getBlockFiles(cluster);
      show("files=" + files);
      corrupt(files);

      assertEquals(null, runner.run(-1));
      String corruptedcontent = runner.run(0, "-ignoreCrc");
      assertEquals(localfcontent.substring(1), corruptedcontent.substring(1));
      assertEquals(localfcontent.charAt(0)+1, corruptedcontent.charAt(0));

    } finally {
      try {dfs.close();} catch (Exception e) {}
      cluster.shutdown();
      testAndDeleteFile(localf);
    }
  }

  public void testLsr() throws Exception {
    Configuration conf = new Configuration();
    conf.set("dfs.namenode.port.list", "0,0");
    MiniMNDFSCluster cluster = new MiniMNDFSCluster(conf, 4, true, null);
    cluster.waitActive();
    cluster.waitDatanodeDie();
    DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();

    try {
      final String root = createTree(dfs, "lsr");
      dfs.mkdirs(new Path(root, "zzz"));
      
      runLsr(new FsShell(conf), root, 0);
      
      final Path sub = new Path(root, "sub");
      dfs.setPermission(sub, new FsPermission((short)0));

      final UserGroupInformation ugi = UserGroupInformation.getCurrentUGI();
      final String tmpusername = ugi.getUserName() + "1";
      UnixUserGroupInformation tmpUGI = new UnixUserGroupInformation(
          tmpusername, new String[] {tmpusername});
      UnixUserGroupInformation.saveToConf(conf,
            UnixUserGroupInformation.UGI_PROPERTY_NAME, tmpUGI);
      String results = runLsr(new FsShell(conf), root, 0);
      assertTrue(results.contains("zzz"));
    } finally {
      dfs.close();
      cluster.shutdown();
    }
  }
  private static String runLsr(final FsShell shell, String root, int returnvalue
      ) throws Exception {
    System.out.println("root=" + root + ", returnvalue=" + returnvalue);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      assertEquals(returnvalue, shell.run(new String[]{"-lsr", root}));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("results:\n" + results);
    return results;
  }
  
  private void testAndDeleteFile(File f) {
    if (null != f && f.exists()) f.delete();
  }
}
