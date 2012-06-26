package com.taobao.adfs.distributed;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.taobao.adfs.distributed.example.ExampleData;

public class DistributedOperationQueueTest {
  DistributedOperationQueue queue = new DistributedOperationQueue();
  static Random r = new Random();

  @Test
  public void testPutAndGetWithOneThread() {
    DistributedOperation op;
    DistributedOperation[] opList;
    for (int i = 0; i < 10000; i++) {
      op = generateOpration();
      queue.add(op);
      if ((i + 1) % 10 == 0) {
        opList = queue.lockAndGetOperations(Thread.currentThread().getId());
        assertTrue("opList.length == " + opList.length, opList.length == 10);
        System.out.println(Arrays.toString(opList));
        queue.deleteAndUnlockOperations(opList);
      }

    }
  }

  @Test
  public void testPutAndGetWithMultiThread() throws InterruptedException {
    Thread[] threads = new Thread[20];
    for(int i = 0; i < threads.length; i++ ){
      threads[i] = new PutAndGetter(queue);
      threads[i].setName("thread"+i);
      threads[i].start();
    }
    for(int i = 0; i < threads.length; i++ ){
      threads[i].join();
    }
  }
  
  /**test when operations of currentThread have previous operations of other thread
   * The order of operation added as follows:
   * T0   op0       op1
   * T1    op3       op1`       op2
   * T2     op2       op1``       op3
   * 
   * T0,T1,T2 respectively is different threads
   * op0,op1,op2,op3 respectively is different operations
   * @throws InterruptedException
   */
  @Test
  public void testOperationQueue() throws InterruptedException{
    Thread t0 = new Getter(queue), t1 =new Getter(queue), t2=new Getter(queue);
    long tid0 = t0.getId(),tid1 = t1.getId(),tid2=t2.getId();
    
    synchronized(queue){
      queue.addByThreadId(generateOprationById(0), tid0);
      queue.addByThreadId(generateOprationById(3), tid1);
      queue.addByThreadId(generateOprationById(2), tid2);
      queue.addByThreadId(generateOprationById(1), tid0);
      queue.addByThreadId(generateOprationById(1), tid1);
      queue.addByThreadId(generateOprationById(1), tid2);
      queue.addByThreadId(generateOprationById(2), tid1);
      queue.addByThreadId(generateOprationById(3), tid2);
    }
      t0.setName("t0");
      t1.setName("t1");
      t2.setName("t2");
      t0.start();
      Thread.sleep(5000);
      t2.start();
      Thread.sleep(10000);
      t1.start();
      t0.join();
      t1.join();
      t2.join();
  }
  
  /**
   * test whether generate resource cycle
   * this case run successfully when run over at some time 
   * @return
   * @throws InterruptedException 
   */
  @Test
  public void testResourceCycle() throws InterruptedException{
    CountDownLatch fire = new CountDownLatch(1);
    CountDownLatch notifier = new CountDownLatch(1);
    CountDownLatch interrupter = new CountDownLatch(1);
    Thread t0 = new NotifyGetter(fire, notifier, interrupter, queue), 
                t1 =new NotifyGetter(fire, notifier, interrupter, queue),
                t2=new NotifyGetter(fire, notifier, interrupter, queue);
    
    long tid0 = t0.getId(),tid1 = t1.getId(),tid2=t2.getId();
    
    synchronized(queue){
      queue.addByThreadId(generateOprationById(0), tid0);
      queue.addByThreadId(generateOprationById(3), tid1);
      queue.addByThreadId(generateOprationById(2), tid2);
      queue.addByThreadId(generateOprationById(1), tid0);
      queue.addByThreadId(generateOprationById(1), tid1);
      queue.addByThreadId(generateOprationById(1), tid2);
      queue.addByThreadId(generateOprationById(2), tid1);
      queue.addByThreadId(generateOprationById(3), tid2);
    }
      t0.setName("t0");
      t1.setName("t1");
      t2.setName("t2");
      t0.start();
      t1.start();
      t2.start();
      //wait enough time to T0 lock successfully and T1 and T2 trying lock
      Thread.sleep(3000);
      interrupter.countDown();
      Thread.sleep(3000);
      fire.countDown();
      t0.join();
      t1.join();
      t2.join();
  }
  
  
  static class PutAndGetter extends Thread{
    DistributedOperationQueue queue;
    public PutAndGetter(DistributedOperationQueue queue){
      this.queue = queue;
    }
    @Override
    public void run() {
      DistributedOperation op;
      DistributedOperation[] opList;
      for (int i = 0; i < 200000; i++) {
        op = generateOpration();
        queue.add(op);
        if ((i + 1) % 10 == 0) {
//          System.out.println(Thread.currentThread().getName() + "index " + i + "begin delete");
//          try {
//            Thread.sleep(1);
//          } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//          }
          opList = queue.lockAndGetOperations(Thread.currentThread().getId());
//          System.out.println("array length:" + opList.length + " " + Arrays.toString(opList));
          queue.deleteAndUnlockOperations(opList);
        }
      }
    }
  }
  
  static class Getter extends Thread{
    DistributedOperationQueue queue;
    public Getter(DistributedOperationQueue queue){
      this.queue = queue;
    }
    public void run(){
      DistributedOperation[] operations = queue.lockAndGetOperations(getId());
      if(getName().equals("t0"))
        assertTrue("operations.length ==" + operations.length, operations.length == 2);
      else if(getName().equals("t1"))
        assertTrue("operations.length ==" + operations.length, operations.length == 1);
      else if(getName().equals("t2"))
        assertTrue("operations.length ==" + operations.length, operations.length == 5);
      System.out.println("threadId" + getId() + "threadName:" + getName() + Arrays.toString(operations));
      queue.deleteAndUnlockOperations(operations);
    }
  }
  
  static class NotifyGetter extends Thread{
    public NotifyGetter(CountDownLatch fire, CountDownLatch notifier, CountDownLatch interrupter, DistributedOperationQueue queue){
      this.fire = fire;
      this.notifier = notifier;
      this.interrupter = interrupter;
      this.queue = queue;
    }
    private CountDownLatch fire;
    private CountDownLatch notifier;
    private CountDownLatch interrupter;
    private DistributedOperationQueue queue;
    DistributedOperation[] operations;
    public void run(){
      try {
        if(getName().equals("t0")){
          operations = queue.lockAndGetOperations(getId());
          System.out.println("threadId" + getId() + "threadName:" + getName() + Arrays.toString(operations));
          notifier.countDown();
          System.out.println(getName() + " is interrupted");
          interrupter.await();
          System.out.println(getName() + " is active again");
        }
        else  {
          notifier.await();
          System.out.println(getName() + " is getting lock");
          operations = queue.lockAndGetOperations(getId());
          System.out.println("threadId" + getId() + "threadName:" + getName() + Arrays.toString(operations));
          System.out.println(getName() + " is interrupted");
          fire.await();
          System.out.println(getName() + " is active again");
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      queue.deleteAndUnlockOperations(operations);
    }
  }
  public static DistributedOperation generateOpration() {
    ExampleData.OperandExample f = new ExampleData.OperandExample("content"+r.nextInt());
    DistributedOperation op =
      new DistributedOperation(DistributedOperation.DistributedOperator.values()[r.nextInt(3)], f);
    return op;
  }
  public static DistributedOperation generateOprationById(long id) {
    ExampleData.OperandExample f = new ExampleData.OperandExample("contend" + r.nextLong(),id);
    DistributedOperation op =
      new DistributedOperation(DistributedOperation.DistributedOperator.values()[r.nextInt(3)], f);
    return op;
  }

}
