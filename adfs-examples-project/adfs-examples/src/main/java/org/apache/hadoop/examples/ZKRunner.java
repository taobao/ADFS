package org.apache.hadoop.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.util.GenericOptionsParser;
// add comment for test git
public class ZKRunner {
  
  private static final String HOME = "/" 
    + ZKRunner.class.getSimpleName() + "/";
  private final ZKClient zkClient;
  private final ExecutorService executor;
  private final int threads;
  private final int tasks;
  private byte[] data;
  
  enum OPSType {
    syncreate,
    asyncreate,
    touch,
    set,
    stat,
    get,
    delsyncreate,
    delasyncreate,
    deltouch
  }
  
  
  interface Operations {
    void operate() throws IOException;
  }
  
  abstract class ConcurrentOperations implements Operations, Runnable {
    
    protected CountDownLatch start, stop;
    protected AtomicInteger counter;
    
    protected ConcurrentOperations(CountDownLatch start, CountDownLatch stop,
        AtomicInteger counter) {
      this.start = start;
      this.stop = stop;
      this.counter = counter;
    }
    
    @Override
    public void run() {
      try {
        start.await();
        operate();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        stop.countDown();
      }
    }

  }
  
  ConcurrentOperations getOps(OPSType type,
      CountDownLatch start, CountDownLatch done, AtomicInteger counter) {
    switch (type) {
    case syncreate:
      return new SynCreate(start, done, counter);
    case asyncreate:
      return new AsynCreate(start, done, counter);
    case touch:
      return new Touch(start, done, counter);
    case set:
      return new Set(start, done, counter);
    case stat:
      return new Stat(start, done, counter);
    case get:
      return new Get(start, done, counter);
    case delsyncreate:
      return new DelSynCreate(start, done, counter);
    case delasyncreate:
      return new DelAsynCreate(start, done, counter);
    case deltouch:
      return new DelTouch(start, done, counter);
    default:
      return null;
    }
  }
  
  class SynCreate extends ConcurrentOperations {
    OPSType type = OPSType.syncreate;
    SynCreate(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.create(sb.toString(), data, false, true);
      }   
    }
  }
  
  class AsynCreate extends ConcurrentOperations {
    OPSType type = OPSType.asyncreate;
    AsynCreate(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.create(sb.toString(), data, false, false);
      }   
    }
  }
  
  class Touch extends ConcurrentOperations {
    OPSType type = OPSType.touch;
    Touch(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.touch(sb.toString(), data, false);
      }
    }
  }
  
  class Set extends ConcurrentOperations {
    OPSType type = OPSType.asyncreate;
    Set(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.setData(sb.toString(), data);
      }
    }
  }
  
  class Get extends ConcurrentOperations {
    OPSType type = OPSType.syncreate;
    Get(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.getData(sb.toString(), null);
      }
    }
  }
  
  class Stat extends ConcurrentOperations {
    OPSType type = OPSType.syncreate;
    Stat(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.exist(sb.toString());
      }
    }
  }
  
  class DelAsynCreate extends ConcurrentOperations {
    OPSType type = OPSType.asyncreate;
    DelAsynCreate(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.delete(sb.toString(), false);
      }
    }
  }
  
  class DelSynCreate extends ConcurrentOperations {
    OPSType type = OPSType.syncreate;
    DelSynCreate(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.delete(sb.toString(), false, true);
      }
    }
  }
  
  class DelTouch extends ConcurrentOperations {
    OPSType type = OPSType.touch;
    DelTouch(CountDownLatch start, CountDownLatch stop, AtomicInteger counter) {
      super(start, stop, counter);
    }
    public void operate() throws IOException {
      StringBuilder sb = new StringBuilder();
      int i;
      while((i = counter.getAndDecrement()) > 0) {
        sb.delete(0, sb.length());
        sb.append(HOME).append(type).append(i);
        zkClient.delete(sb.toString(), false, true);
      }
    }
  }
  
  class MeasureOperates {
    
    public long measure(OPSType type) {
      final CountDownLatch startSignal = new CountDownLatch(1);
      final CountDownLatch doneSignal = new CountDownLatch(threads);
      final AtomicInteger counts = new AtomicInteger(tasks);
      for(int i = 0; i < threads; i++) {
        executor.execute(getOps(type, startSignal, doneSignal, counts));
      }
      
      long start = System.currentTimeMillis();
      startSignal.countDown();
      try {
        doneSignal.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return (System.currentTimeMillis() - start);
    }
  }
  
  long measure(OPSType type) {
    return new MeasureOperates().measure(type);
  }
  
  ZKRunner(Configuration conf, 
      int ct, int size, int ths) throws IOException {
    zkClient = ZKClient.getInstance(conf);
    tasks = ct;
    data = ByteBuffer.allocate(size).array();
    threads = ths;
    executor = Executors.newFixedThreadPool(threads);
    init();
  }
  
  private void init() throws IOException {
    zkClient.create("/" + ZKRunner.class.getSimpleName(), 
        new byte[0], false, true);
    clean();
  }
  
  private void clean() throws IOException {
    String normalPath = "/" + ZKRunner.class.getSimpleName();
    int count = 0;
    long start = System.currentTimeMillis();
    List<String> children = zkClient.getChildren(normalPath, null);
    long endcld = System.currentTimeMillis();
    if(children != null) {
      for(Iterator<String> iter = children.iterator(); iter.hasNext();) {
        zkClient.delete(normalPath + "/" + iter.next(), false);
        count++;
      }
    }
    long enddel = System.currentTimeMillis();
    System.out.println(">>> CLEAN: nodes=" + count + ", getTime(msec)=" + (endcld-start)
        + ", delTime(msec)=" + (enddel-endcld) + " <<<");
  }
  
  public void run() throws IOException {

    long syncreate = measure(OPSType.syncreate);
    long asyncreate = measure(OPSType.asyncreate);
    long touch = measure(OPSType.touch);
    long set = measure(OPSType.set);
    long stat = measure(OPSType.stat);
    long get = measure(OPSType.get);
    long delsyn = measure(OPSType.delsyncreate);
    long delasyn = measure(OPSType.delasyncreate);
    long deltch = measure(OPSType.deltouch);
    
    System.out.println("CREATE(SYN)         : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + syncreate + ", ops=" + safeCal(syncreate));
    System.out.println("CREATE(ASYN)        : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + asyncreate + ", ops=" + safeCal(asyncreate));
    System.out.println("TOUCH(SYN)          : threads=" + threads + ", count=" + tasks +
        ", time(msec)=" + touch + ", ops=" + safeCal(touch));
    System.out.println("SET(ASYN)           : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + set + ", ops=" + safeCal(set));
    System.out.println("STAT(SYN)           : threads=" + threads + ", count=" + tasks +
        ", time(msec)=" + stat + ", ops=" + safeCal(stat));
    System.out.println("GET(SYN)            : threads=" + threads + ", count=" + tasks +
        ", time(msec)=" + get + ", ops=" + safeCal(get));
    System.out.println("DELETE(SYN)         : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + delsyn + ", ops=" + safeCal(delsyn));
    System.out.println("DELETE(ASYN)        : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + delasyn + ", ops=" + safeCal(delasyn));
    System.out.println("DELETE TOUCH (SYN)  : threads=" + threads + ", count=" + tasks + 
        ", time(msec)=" + deltch + ", ops=" + safeCal(deltch));
    
    clean();
    
  } 
  
  private String safeCal(long val) {
    return (val>0?Long.toString((tasks*1000/val)):"INF");
  }

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
    System.err.println("Usage: zkclienttester <servers> <count> <size> <threads>");
    System.exit(2);
    }
    try {
      conf.set("zookeeper.server.list", otherArgs[0]);
      ZKRunner test = new ZKRunner(conf, Integer.parseInt(otherArgs[1]), 
          Integer.parseInt(otherArgs[2]), Integer.parseInt(otherArgs[3]));
      test.run();
    }catch(IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
