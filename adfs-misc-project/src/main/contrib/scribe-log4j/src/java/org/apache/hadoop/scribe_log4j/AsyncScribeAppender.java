package org.apache.hadoop.scribe_log4j;

import org.apache.log4j.AsyncAppender;

/*
 * An asynchronous version of {@link ScribeAppender}, which extends
 * Log4j's AsyncAppender.
 */
public class AsyncScribeAppender extends AsyncAppender {

  private String hostname;
  private String scribeHost;
  private int scribePort;
  private String scribeCategory;

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getScribeHost() {
    return scribeHost;
  }

  public void setScribeHost(String scribeHost) {
    this.scribeHost = scribeHost;
  }

  public int getScribePort() {
    return scribePort;
  }

  public void setScribePort(int scribePort) {
    this.scribePort = scribePort;
  }

  public String getScribeCategory() {
    return scribeCategory;
  }

  public void setScribeCategory(String scribeCategory) {
    this.scribeCategory = scribeCategory;
  }

  @Override
  public void activateOptions() {
    super.activateOptions();
    synchronized(this) {
      ScribeAppender scribeAppender = new ScribeAppender();
      scribeAppender.setLayout(getLayout());
      scribeAppender.setHostname(getHostname());
      scribeAppender.setScribeHost(getScribeHost());
      scribeAppender.setScribePort(getScribePort());
      scribeAppender.setScribeCategory(getScribeCategory());
      scribeAppender.activateOptions();
      addAppender(scribeAppender);
    }
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

}