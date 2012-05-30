package com.taobao.adfs.distributed;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.taobao.adfs.distributed.example.ExampleData;

public class DistributedOperationQueueTest {
  DistributedOperationQueue queue = new DistributedOperationQueue();

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
        queue.deleteAndUnlockOperations(Thread.currentThread().getId());
      }

    }
  }

  public static DistributedOperation generateOpration() {
    Random r = new Random();
    ExampleData.OperandExample f = new ExampleData.OperandExample("contend" + r.nextLong());
    DistributedOperation op =
      new DistributedOperation(DistributedOperation.DistributedOperator.values()[r.nextInt(3)], f);
    return op;
  }

}
