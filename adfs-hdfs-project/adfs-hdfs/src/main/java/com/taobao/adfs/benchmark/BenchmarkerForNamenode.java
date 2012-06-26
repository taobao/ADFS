package com.taobao.adfs.benchmark;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.mail.SimpleEmail;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:jiwan@taobao.com>jiwan</a>
 */
public class BenchmarkerForNamenode {
  public static final Logger logger = LoggerFactory.getLogger(BenchmarkerForNamenode.class);
  Configuration conf = null;
  Operation[] operations = null;
  FileSystem fs = null;

  BenchmarkerForNamenode(Configuration conf) throws Exception {
    this.conf = (conf == null) ? new Configuration() : conf;
    if (conf.getInt("system.args.size", 0) == 0) conf.set("tester.benchmarker.help", "true");
    conf.set("fs.default.name", conf.get("fs.default.name", "hdfs://localhost:8020"));
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    if (conf.get("tester.benchmarker.help") != null) {
      printUsage();
      return;
    } else showParameters(false);

    fs = FileSystem.get(conf);
  }

  static void printUsage() {
    System.out.println("usage: " + BenchmarkerForNamenode.class.getSimpleName());
    System.out.println("  [-append.number              number] -- default is 0");
    System.out.println("  [-create.number              number] -- default is 0");
    System.out.println("  [-delete.number              number] -- default is 0");
    System.out.println("  [-mkdirs.number              number] -- default is 0");
    System.out.println("  [-rename.number              number] -- default is 0");
    System.out.println("  [-getblockLocations.number   number] -- default is 0");
    System.out.println("  [-getlisting.number          number] -- default is 0");
    System.out.println("  [-getfileinfo.number         number] -- default is 0");
    System.out.println("  [-setpermission.number       number] -- default is 0");
    System.out.println("  [-setreplication.number      number] -- default is 0");
    System.out.println("  [-prepare.threads            number] -- default is 100");
    System.out.println("  [-prepare.fast           true/false] -- default is true");
    System.out.println("  [-prepare.cleanup        true/false] -- default is true");
    System.out.println("  [-execute.threads            number] -- default is 100");
    System.out.println("  [-execute.mixed          true/false] -- default is false");
    System.out.println("  [-files.per.dir              number] -- default is 100");
    System.out.println("  [-data.size                  number] -- default is 1024");
    System.out.println("  [-show.files             true/false] -- default is false");
    System.out.println("  [-environment.state.manager  string] -- default is \"\"");
    System.out.println("  [-environment.zookeeper      string] -- default is \"\"");
    System.out.println("  [-environment.namenode       string] -- default is \"\"");
    System.out.println("  [-environment.datanode       string] -- default is \"\"");
    System.out.println("  [-environment.benchmarker    string] -- default is \"\"");
    System.out.println("  [-help                             ] -- show help");
    System.out
        .println("  example: bin/hadoop benchmark -Ddistributed.client.enabled=false -Dfs.default.name=hdfs://10.232.98.98:9900 -Dtester.benchmarker.create.number=1000 -Dtester.benchmarker.data.size=102400");
  }

  String showParameter(String keyName, String defaultValue, int keyWidth, boolean noLog) {
    conf.set(keyName, conf.get(keyName, defaultValue));
    String postFixOfKey = String.format("%" + (keyWidth - keyName.length()) + "s", "");
    String parameter = keyName + postFixOfKey + "=" + conf.get(keyName);
    if (!noLog) logger.info(parameter);
    return parameter;
  }

  String showParametersForOperations(int widthOfKey, boolean noLog) {
    StringBuilder strinbBuilder = new StringBuilder();
    for (OperationType operationType : OperationType.values()) {
      String keyOfOperation = "tester.benchmarker." + operationType.toString().toLowerCase() + ".number";
      strinbBuilder.append(showParameter(keyOfOperation, "0", widthOfKey, noLog)).append('\n');
    }
    return strinbBuilder.toString();
  }

  String showParameters(boolean noLog) {
    int keyWidth = 50;
    StringBuilder strinbBuilder = new StringBuilder();
    String showParameterStart = "------------------------------show parameters start------------------------------";
    strinbBuilder.append(showParameterStart).append('\n');
    if (!noLog) logger.info(showParameterStart);
    strinbBuilder.append(showParametersForOperations(keyWidth, noLog));
    strinbBuilder.append(showParameter("tester.benchmarker.prepare.threads", "100", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.prepare.fast", "true", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.prepare.cleanup", "true", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.execute.threads", "100", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.execute.mixed", "true", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.files.per.dir", "100", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.data.size", "1024", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.show.files", "false", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.environment.state.manager", "", keyWidth, noLog)).append(
        '\n');
    strinbBuilder.append(showParameter("tester.benchmarker.environment.zookeeper", "", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.environment.namenode", "", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.environment.datanode", "", keyWidth, noLog)).append('\n');
    strinbBuilder.append(showParameter("tester.benchmarker.environment.benchmarker", "", keyWidth, noLog)).append('\n');
    String showParameterEnd = "------------------------------show parameters end--------------------------------";
    if (!noLog) logger.info(showParameterEnd);
    return strinbBuilder.toString();
  }

  int countOperation() {
    int count = 0;
    for (OperationType operationType : OperationType.values()) {
      count += conf.getInt("tester.benchmarker." + operationType.toString().toLowerCase() + ".number", 0);
    }
    return count;
  }

  void generateOperations() throws Exception {
    logger.info("generateOperations                                is doing");
    operations = new Operation[countOperation()];
    int indexOfOperation = 0;
    // generate for all operation type
    for (OperationType operationType : OperationType.values()) {
      // generate for this operation type
      Random random = new Random();
      int number = conf.getInt("tester.benchmarker." + operationType.toString().toLowerCase() + ".number", 0);
      for (int i = 0; i < number; ++i) {
        int indexOfThisOperation = indexOfOperation + i;
        if (conf.getBoolean("tester.benchmarker.execute.mixed", true)) {
          // generate for each operation randomly
          indexOfThisOperation = random.nextInt(operations.length);
          while (operations[indexOfThisOperation] != null) {
            indexOfThisOperation = ++indexOfThisOperation % operations.length;
          }
        }
        operations[indexOfThisOperation] = Operation.getActualOperation(fs, operationType, indexOfThisOperation, conf);
      }
      indexOfOperation += number;
    }
  }

  void prepareOperations() throws Exception {
    logger.info("cleanup                                           is doing");
    if (conf.getBoolean("tester.benchmarker.prepare.cleanup", true)) Operation.cleanup(fs);
    logger.info("prepareOperations                                 is doing");
    Operation.fastPrepare = conf.getBoolean("tester.benchmarker.prepare.fast", true);
    prepareOrExecuteOperations(false);
  }

  void executeOperations() throws Exception {
    logger.info("executeOperations                                 is doing");
    prepareOrExecuteOperations(true);
  }

  void prepareOrExecuteOperations(boolean execute) throws Exception {
    OperationThread.count.set(0);
    List<OperationThread> operationThreads = new ArrayList<OperationThread>();
    for (int i = 0; i < conf.getInt("tester.benchmarker." + (execute ? "execute" : "prepare") + ".threads", 100); ++i) {
      OperationThread operationThread = new OperationThread(operations, execute);
      operationThreads.add(operationThread);
      operationThread.start();
    }

    while (!operationThreads.isEmpty()) {
      for (OperationThread operationThread : operationThreads.toArray(new OperationThread[0])) {
        if (!operationThread.isAlive()) operationThreads.remove(operationThread);
      }
      int countOfProcessedOperations = OperationThread.count.get();
      if (countOfProcessedOperations <= operations.length)
        logger.info("  " + (execute ? "execute" : "prepare") + "d operations=" + OperationThread.count.get());
      Thread.sleep(1000);
    }
  }

  void showFiles(String path) {
    if (!conf.getBoolean("tester.benchmarker.show.files", false)) return;

    try {
      // show this file
      logger.info("showFiles                                         is doing");
      FileStatus fileStatus = fs.getFileStatus(new Path(path));
      String fileInfo = "";
      fileInfo += String.format((fileStatus.isDir() ? "d" : "-") + fileStatus.getPermission() + " ");
      fileInfo += String.format("%s ", (!fileStatus.isDir() ? fileStatus.getReplication() : "-"));
      fileInfo += String.format("%s ", fileStatus.getOwner());
      fileInfo += String.format("%s ", fileStatus.getGroup());
      fileInfo += String.format("%d ", fileStatus.getLen());
      fileInfo += String.format(fileStatus.getPath().toString());
      logger.info(fileInfo);

      // recursive
      if (fileStatus.isDir()) {
        FileStatus[] files = fs.listStatus(fileStatus.getPath());
        for (FileStatus file : files) {
          showFiles(file.getPath().toString());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void analyzeResult() {
    // statistics
    long totalTimeStart = Long.MAX_VALUE;
    long totalTimeEnd = Long.MIN_VALUE;

    for (Operation operation : operations) {
      if (operation.startTime <= 0 || operation.endTime <= 0) continue;

      // statistics for total operations
      if (totalTimeStart > operation.startTime) {
        totalTimeStart = operation.startTime;
      }
      if (totalTimeEnd < operation.endTime) {
        totalTimeEnd = operation.endTime;
      }

      // statistics for each operation type

      String operationString = operation.type.toString().toLowerCase();
      String keyForStartTimeOfThisOperation = "tester.benchmarker." + operationString + ".time.start";
      String keyForEndTimeOfThisOperation = "tester.benchmarker." + operationString + ".time.end";
      String keyForSumTimeOfThisType = "tester.benchmarker." + operationString + ".time.sum";
      String keyForMaxTimeOfThisType = "tester.benchmarker." + operationString + ".time.max";
      String keyForMinTimeOfThisOperation = "tester.benchmarker." + operationString + ".time.min";

      if (conf.getLong(keyForStartTimeOfThisOperation, Long.MAX_VALUE) > operation.startTime)
        conf.setLong(keyForStartTimeOfThisOperation, operation.startTime);
      if (conf.getLong(keyForEndTimeOfThisOperation, Long.MIN_VALUE) < operation.endTime)
        conf.setLong(keyForEndTimeOfThisOperation, operation.endTime);
      conf.setLong(keyForSumTimeOfThisType, operation.endTime - operation.startTime
          + conf.getLong(keyForSumTimeOfThisType, 0));
      if (conf.getLong(keyForMaxTimeOfThisType, Long.MIN_VALUE) < operation.endTime - operation.startTime)
        conf.setLong(keyForMaxTimeOfThisType, operation.endTime - operation.startTime);
      if (conf.getLong(keyForMinTimeOfThisOperation, Long.MAX_VALUE) > operation.endTime - operation.startTime) {
        conf.setLong(keyForMinTimeOfThisOperation, operation.endTime - operation.startTime);
      }
    }

    for (OperationType operationType : OperationType.values()) {
      String operationString = operationType.toString().toLowerCase();
      String keyForNumberOfThisType = "tester.benchmarker." + operationString + ".number";
      String keyForSumTimeOfThisType = "tester.benchmarker." + operationString + ".time.sum";
      String keyForAverageTimeOfThisType = "tester.benchmarker." + operationString + ".time.average";
      String keyForStartTimeOfThisType = "tester.benchmarker." + operationString + ".time.start";
      String keyForEndTimeOfThisType = "tester.benchmarker." + operationString + ".time.end";
      String keyForElapsedTimeOfThisType = "tester.benchmarker." + operationString + ".time.elapsed";
      String keyForOpsOfThisType = "tester.benchmarker." + operationString + ".ops";

      long operationNumberForThisType = conf.getLong(keyForNumberOfThisType, 0);
      if (operationNumberForThisType <= 0) continue;
      long sumTimeForThisType = conf.getLong(keyForSumTimeOfThisType, 0);
      float averageTimeForThisType = (float) (sumTimeForThisType * 1.0 / operationNumberForThisType);
      long startTimeForThisType = conf.getLong(keyForStartTimeOfThisType, Long.MAX_VALUE);
      long endTimeForThisType = conf.getLong(keyForEndTimeOfThisType, Long.MIN_VALUE);
      long elapsedTimeForThisType = endTimeForThisType - startTimeForThisType;
      float opsForThisType = (float) (operationNumberForThisType * 1000.0 / elapsedTimeForThisType);

      conf.setFloat(keyForAverageTimeOfThisType, averageTimeForThisType);
      conf.setLong(keyForElapsedTimeOfThisType, elapsedTimeForThisType);
      conf.setFloat(keyForOpsOfThisType, opsForThisType);
    }

    conf.setLong("tester.benchmarker.total.number", countOperation());
    conf.setLong("tester.benchmarker.total.time.elapsed", totalTimeEnd - totalTimeStart);
    conf.setFloat("tester.benchmarker.total.ops", (float) (countOperation() * 1000.0 / (totalTimeEnd - totalTimeStart)));
  }

  void showResult() throws Exception {
    // info for total analysis
    long totalNumber = conf.getLong("tester.benchmarker.total.number", 0);
    long totalTimeElapsed = conf.getLong("tester.benchmarker.total.time.elapsed", 0);
    float totalOps = conf.getFloat("tester.benchmarker.total.ops", 0);
    Utilities.logInfo(logger, "------------------------------show result start----------------------------------");

    if (conf.getBoolean("tester.benchmarker.execute.mixed", true)) {
      Utilities.logInfo(logger, "tester.benchmarker.total.number                   =", totalNumber);
      Utilities.logInfo(logger, "tester.benchmarker.total.ops                      =", String.format("%.2f", totalOps));
      Utilities.logInfo(logger, "tester.benchmarker.total.time.elapsed             =", String.format("%.2fms",
          totalTimeElapsed * 1.0));
      Utilities.logInfo(logger, "---------------------------------------------------------------------------------");
    }
    // info for this type analysis
    boolean firstOperation = true;
    for (OperationType operationType : OperationType.values()) {
      String operationString = operationType.toString().toLowerCase();
      String operationSpaces = String.format("%" + (18 - operationString.length()) + "s", "");

      String keyForNumberOfThisType = "tester.benchmarker." + operationString + ".number";
      String keyForElapsedTimeOfThisType = "tester.benchmarker." + operationString + ".time.elapsed";
      String keyForSumTimeOfThisType = "tester.benchmarker." + operationString + ".time.sum";
      String keyForAverageTimeOfThisType = "tester.benchmarker." + operationString + ".time.average";
      String keyForOpsOfThisType = "tester.benchmarker." + operationString + ".ops";
      String keyForMinTimeOfThisType = "tester.benchmarker." + operationString + ".time.min";
      String keyForMaxTimeOfThisType = "tester.benchmarker." + operationString + ".time.max";

      long operationNumberForThisType = conf.getLong(keyForNumberOfThisType, 0);
      if (operationNumberForThisType <= 0) continue;
      float opsForThisType = conf.getFloat(keyForOpsOfThisType, 0);
      long elapsedTimeForThisType = conf.getLong(keyForElapsedTimeOfThisType, 0);
      long sumTimeForThisType = conf.getLong(keyForSumTimeOfThisType, 0);
      float averageTimeForThisType = conf.getFloat(keyForAverageTimeOfThisType, 0);
      long minTimeForThisType = conf.getLong(keyForMinTimeOfThisType, 0);
      long maxTimeForThisType = conf.getLong(keyForMaxTimeOfThisType, 0);

      if (!firstOperation)
        Utilities.logInfo(logger, "---------------------------------------------------------------------------------");
      firstOperation = false;

      Utilities.logInfo(logger, "tester.benchmarker.", operationString, ".number", operationSpaces, "      =",
          operationNumberForThisType);
      if (!conf.getBoolean("tester.benchmarker.execute.mixed", true)) {
        Utilities.logInfo(logger, "tester.benchmarker." + operationString + ".ops" + operationSpaces + "         ="
            + String.format("%.2f", opsForThisType));
        Utilities.logInfo(logger, "tester.benchmarker." + operationString + ".time.elapsed" + operationSpaces + "="
            + String.format("%.2fms", elapsedTimeForThisType * 1.0));
      }
      Utilities.logInfo(logger, "tester.benchmarker." + operationString + ".time.sum" + operationSpaces + "    ="
          + String.format("%.2fms", sumTimeForThisType * 1.0));
      Utilities.logInfo(logger, "tester.benchmarker.", operationString, ".time.average", operationSpaces, "=", String
          .format("%.2fms", averageTimeForThisType));
      Utilities.logInfo(logger, "tester.benchmarker.", operationString, ".time.min", operationSpaces, "    =", String
          .format("%.2fms", minTimeForThisType * 1.0));
      Utilities.logInfo(logger, "tester.benchmarker.", operationString, ".time.max", operationSpaces, "    =", String
          .format("%.2fms", maxTimeForThisType * 1.0));
    }
  }

  String getResultForMail() {
    StringBuilder result = new StringBuilder();
    result.append(showParameters(true));
    result.append("------------------------------show result start----------------------------------\n");

    // info for total analysis
    long totalNumber = conf.getLong("tester.benchmarker.total.number", 0);
    long totalTimeElapsed = conf.getLong("tester.benchmarker.total.time.elapsed", 0);
    float totalOps = conf.getFloat("tester.benchmarker.total.ops", 0);

    if (conf.getBoolean("tester.benchmarker.execute.mixed", true)) {
      result.append("tester.benchmarker.total.number                   =" + totalNumber + "\n");
      result.append("tester.benchmarker.total.ops                      =" + String.format("%.2f", totalOps) + "\n");
      result.append("tester.benchmarker.total.time.elapsed             ="
          + String.format("%.2fms", totalTimeElapsed * 1.0) + "\n");
      result.append("---------------------------------------------------------------------------------" + "\n");
    }
    // info for this type analysis
    for (OperationType operationType : OperationType.values()) {
      String operationString = operationType.toString().toLowerCase();
      String operationSpaces = String.format("%" + (18 - operationString.length()) + "s", "");

      String keyForNumberOfThisType = "tester.benchmarker." + operationString + ".number";
      String keyForElapsedTimeOfThisType = "tester.benchmarker." + operationString + ".time.elapsed";
      String keyForSumTimeOfThisType = "tester.benchmarker." + operationString + ".time.sum";
      String keyForAverageTimeOfThisType = "tester.benchmarker." + operationString + ".time.average";
      String keyForOpsOfThisType = "tester.benchmarker." + operationString + ".ops";
      String keyForMinTimeOfThisType = "tester.benchmarker." + operationString + ".time.min";
      String keyForMaxTimeOfThisType = "tester.benchmarker." + operationString + ".time.max";

      long operationNumberForThisType = conf.getLong(keyForNumberOfThisType, 0);
      if (operationNumberForThisType <= 0) continue;
      float opsForThisType = conf.getFloat(keyForOpsOfThisType, 0);
      long elapsedTimeForThisType = conf.getLong(keyForElapsedTimeOfThisType, 0);
      long sumTimeForThisType = conf.getLong(keyForSumTimeOfThisType, 0);
      float averageTimeForThisType = conf.getFloat(keyForAverageTimeOfThisType, 0);
      long minTimeForThisType = conf.getLong(keyForMinTimeOfThisType, 0);
      long maxTimeForThisType = conf.getLong(keyForMaxTimeOfThisType, 0);

      result.append("tester.benchmarker." + operationString + ".number" + operationSpaces + "      ="
          + operationNumberForThisType + "\n");
      if (!conf.getBoolean("tester.benchmarker.execute.mixed", true)) {
        result.append("tester.benchmarker." + operationString + ".ops" + operationSpaces + "         ="
            + String.format("%.2f", opsForThisType) + "\n");
        result.append("tester.benchmarker." + operationString + ".time.elapsed" + operationSpaces + "="
            + String.format("%.2fms", elapsedTimeForThisType * 1.0) + "\n");
      }
      result.append("tester.benchmarker." + operationString + ".time.sum" + operationSpaces + "    ="
          + String.format("%.2fms", sumTimeForThisType * 1.0) + "\n");
      result.append("tester.benchmarker." + operationString + ".time.average" + operationSpaces + "="
          + String.format("%.2fms", averageTimeForThisType) + "\n");
      result.append("tester.benchmarker." + operationString + ".time.min" + operationSpaces + "    ="
          + String.format("%.2fms", minTimeForThisType * 1.0) + "\n");
      result.append("tester.benchmarker." + operationString + ".time.max" + operationSpaces + "    ="
          + String.format("%.2fms", maxTimeForThisType * 1.0) + "\n");

      result.append("---------------------------------------------------------------------------------" + "\n");
    }

    return result.toString();
  }

  void sendMail() throws Exception {
    Utilities.logInfo(logger, "------------------------------send mail start------------------------------------");
    logger.info("sendMail                                          is doing");
    String mailFrom = "admin@tbdfs.com";
    String mailToString = conf.get("tester.benchmarker.result.mail.to", "").trim();
    Set<String> mailTos = new HashSet<String>(Arrays.asList(mailToString.split(",")));
    mailTos.remove("");
    mailTos.add("admin@tbdfs.com");
    String version = conf.get("tester.benchmarker.version", "TBDFS");
    logger.info("tester.benchmarker.result.mail.from               =" + mailFrom);
    logger.info("tester.benchmarker.result.mail.to                 =" + mailTos.toString().replaceAll("[ \\[\\]]", ""));
    logger.info("tester.benchmarker.version                        =" + version);
    SimpleEmail simpleEmail = new SimpleEmail();
    simpleEmail.setHostName("smtp.tbdfs.com");
    simpleEmail.setAuthentication("admin@tbdfs.com", "tbdfsadmin");
    simpleEmail.setFrom(mailFrom);
    for (String mailTo : mailTos) {
      mailTo = mailTo.trim();
      simpleEmail.addTo(mailTo);
    }
    simpleEmail.setSubject("Performance test result for " + version);
    simpleEmail.setCharset("GB2312");
    simpleEmail.setMsg(getResultForMail());
    simpleEmail.send();
    logger.info("sendMail                                          is done");
  }

  void execute() throws Exception {
    if (fs == null) return;
    generateOperations();
    prepareOperations();
    executeOperations();
    showFiles(Operation.baseDirName);
    analyzeResult();
    showResult();
    sendMail();
    close();
  }

  void close() throws IOException {
    fs.close();
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("system.args.size", String.valueOf(args.length));
    Utilities.parseVmArgs(args, "-Dtester.benchmarker.");
    runBenchmark(Utilities.loadConfiguration("distributed-server"));
  }

  public static void runBenchmark(Configuration conf) throws Exception {
    new BenchmarkerForNamenode(conf).execute();
  }

  static public class OperationThread extends Thread {
    static final AtomicInteger count = new AtomicInteger(0);
    Operation[] operations = null;
    boolean doExecute = true;

    OperationThread(Operation[] operations, boolean doExecute) {
      this.operations = operations;
      this.doExecute = doExecute;
    }

    public void run() {
      try {
        execute();
      } catch (Exception e) {
        logger.error(getName() + " failed: \n" + StringUtils.stringifyException(e));
      }
    }

    void execute() throws Exception {
      int operationIndex = 0;
      while ((operationIndex = count.getAndIncrement()) < operations.length) {
        Operation operation = operations[operationIndex];
        if (doExecute) operation.execute();
        else operation.prepare();
      }
    }
  }

  public enum OperationType {
    APPEND, CREATE, DELETE, MKDIRS, RENAME, GETBLOCKLOCATIONS, GETFILEINFO, GETLISTING, SETREPLICATION;
  };

  static abstract public class Operation {
    Configuration conf = null;
    static final String clientName = BenchmarkerForNamenode.class.getSimpleName();
    static final String baseDirName = "/" + BenchmarkerForNamenode.class.getSimpleName().toLowerCase() + "/"
        + Utilities.getCurrentTime();
    static boolean fastPrepare = true;
    FileSystem fs = null;
    OperationType type = null;
    int id = 0;
    boolean overwrite = false;
    short replication = 1;
    int blockSize = 67108864;
    Path srcFilePath = null;
    Path desFilePath = null;
    long startTime = 0;
    long endTime = 0;
    int dataSize = 1024;

    // default parameters
    public Operation(FileSystem fs, OperationType operationType, int id, Configuration conf) throws Exception {
      this(fs, operationType, id, true, (short) 1, 67108864, conf);
    }

    public Operation(FileSystem fs, OperationType type, int id, boolean overwrite, short replication, int blockSize,
        Configuration conf) throws Exception {
      this.fs = fs;
      this.type = type;
      this.id = id;
      this.overwrite = overwrite;
      this.replication = replication;
      this.blockSize = blockSize;
      this.conf = conf;
      dataSize = conf.getInt("tester.benchmarker.data.size", 1024);
    }

    void prepare() throws Exception {
      int maxFileNumberInFolder = conf.getInt("tester.benchmarker.files.per.dir", 1);

      Path filePath =
          new Path(baseDirName + "/" + type.toString().toLowerCase() + "/folder-"
              + String.format("%010d", id / maxFileNumberInFolder));
      if (id % maxFileNumberInFolder == 0) fs.mkdirs(filePath);

      String idString = String.format("%010d", id);
      srcFilePath = new Path(filePath, "srcFile-" + idString);
      desFilePath = new Path(filePath, "desFile-" + idString);

      prepareInternal();
    }

    protected abstract void prepareInternal() throws Exception;

    void execute() throws Exception {
      startTime = System.currentTimeMillis();
      excuteInternal();
      endTime = System.currentTimeMillis();
    }

    protected abstract void excuteInternal() throws Exception;

    static void cleanup(FileSystem fs) throws Exception {
      fs.delete(new Path("/" + BenchmarkerForNamenode.class.getSimpleName().toLowerCase()), true);
    }

    static Map<Integer, byte[]> bytesMap = new ConcurrentHashMap<Integer, byte[]>();

    static byte[] getBytes(int size) {
      byte[] bytes = bytesMap.get(size);
      if (bytes != null) return bytes;
      bytesMap.put(size, bytes = new byte[size]);
      for (int i = 0; i < size; ++i) {
        bytes[i] = '0';
      }
      return bytes;
    }

    static Operation getActualOperation(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      switch (type) {
      case APPEND:
        return new OperationAppend(fs, type, id, conf);
      case CREATE:
        return new OperationCreate(fs, type, id, conf);
      case RENAME:
        return new OperationRename(fs, type, id, conf);
      case DELETE:
        return new OperationDelete(fs, type, id, conf);
      case MKDIRS:
        return new OperationMkdirs(fs, type, id, conf);
      case GETBLOCKLOCATIONS:
        return new OperationGetBlockLocations(fs, type, id, conf);
      case GETFILEINFO:
        return new OperationGetFileInfo(fs, type, id, conf);
      case GETLISTING:
        return new OperationGetListing(fs, type, id, conf);
      case SETREPLICATION:
        return new OperationSetReplication(fs, type, id, conf);
      }
      return null;
    }
  }

  static public class OperationAppend extends Operation {
    OperationAppend(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
    }

    protected void excuteInternal() throws Exception {
      FSDataOutputStream outputStream = fs.append(srcFilePath, 4096);
      if (dataSize > 0) outputStream.write(getBytes(dataSize));
      outputStream.close();
    }
  }

  static public class OperationCreate extends Operation {
    OperationCreate(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
    }

    protected void excuteInternal() throws Exception {
      FSDataOutputStream outputStream = fs.create(srcFilePath, overwrite, 4096, replication, blockSize);
      if (dataSize > 0) outputStream.write(getBytes(dataSize));
      outputStream.close();
    }
  }

  static class OperationRename extends Operation {
    OperationRename(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
      // read into cache
      fs.getFileStatus(srcFilePath);
    }

    protected void excuteInternal() throws Exception {
      fs.rename(srcFilePath, desFilePath);
    }
  }

  static class OperationDelete extends Operation {
    OperationDelete(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
      // read into cache
      fs.getFileStatus(srcFilePath);
    }

    protected void excuteInternal() throws Exception {
      fs.delete(srcFilePath, true);
    }
  }

  static class OperationMkdirs extends Operation {
    OperationMkdirs(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
    }

    protected void excuteInternal() throws Exception {
      fs.mkdirs(srcFilePath);
    }
  }

  // setReplication
  static class OperationSetReplication extends Operation {
    OperationSetReplication(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
      // read into cache
      fs.getFileStatus(srcFilePath);
    }

    protected void excuteInternal() throws Exception {
      fs.setReplication(srcFilePath, (short) 3);
    }
  }

  static class OperationGetListing extends Operation {
    OperationGetListing(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      // only create once
      if (id > 0) {
        String filePath = baseDirName + "/" + type.toString().toLowerCase() + "/folder-" + String.format("%010d", 0);
        srcFilePath = new Path(filePath, "srcFile-" + String.format("%010d", 0));
        desFilePath = new Path(filePath, "desFile-" + String.format("%010d", 0));
        return;
      }
      fs.mkdirs(srcFilePath);

      int maxFileNumberInFolder = conf.getInt("tester.operation.files.per.dir", 1);
      for (Integer i = 0; i < maxFileNumberInFolder; ++i) {
        Path fileInFolder = new Path(srcFilePath, "fileInFolder-" + String.format("%010d", i));
        fs.create(fileInFolder, overwrite, 4096, replication, blockSize).close();
      }
      // read into cache
      if (id % 8 != 0 && id % 8 != 1) fs.listStatus(srcFilePath);
    }

    protected void excuteInternal() throws Exception {
      fs.listStatus(srcFilePath);
    }
  }

  // getBlockLocations
  static class OperationGetBlockLocations extends Operation {
    OperationGetBlockLocations(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      // only create once
      if (id > 0 && fastPrepare) {
        String filePath = baseDirName + "/" + type.toString().toLowerCase() + "/folder-" + String.format("%010d", 0);
        srcFilePath = new Path(filePath, "srcFile-" + String.format("%010d", 0));
        desFilePath = new Path(filePath, "desFile-" + String.format("%010d", 0));
        return;
      }
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
      // read into cache
      fs.getFileBlockLocations(new FileStatus(0, false, replication, blockSize, 0, srcFilePath), 0, 0);
    }

    protected void excuteInternal() throws Exception {
      fs.getFileBlockLocations(new FileStatus(0, false, replication, blockSize, 0, srcFilePath), 0, 0);
    }
  }

  // getFileInfo
  static class OperationGetFileInfo extends Operation {
    OperationGetFileInfo(FileSystem fs, OperationType type, int id, Configuration conf) throws Exception {
      super(fs, type, id, conf);
    }

    protected void prepareInternal() throws Exception {
      // only create once
      if (id > 0 && fastPrepare) {
        String filePath = baseDirName + "/" + type.toString().toLowerCase() + "/folder-" + String.format("%010d", 0);
        srcFilePath = new Path(filePath, "srcFile-" + String.format("%010d", 0));
        desFilePath = new Path(filePath, "desFile-" + String.format("%010d", 0));
        return;
      }
      fs.create(srcFilePath, overwrite, 4096, replication, blockSize).close();
      // read into cache
      fs.getFileStatus(srcFilePath);
    }

    protected void excuteInternal() throws Exception {
      fs.getFileStatus(srcFilePath);
    }
  }
}
