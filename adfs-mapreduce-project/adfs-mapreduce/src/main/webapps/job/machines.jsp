<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String type = request.getParameter("type");
%>
<%!
  public void generateTaskTrackerTable(JspWriter out,
                                       String type,
                                       JobTracker tracker) throws IOException {
    Collection c;
    if (("blacklisted").equals(type)) {
      c = tracker.blacklistedTaskTrackers();
    } else if (("active").equals(type)) {
      c = tracker.activeTaskTrackers();
    } else {
      c = tracker.taskTrackers();
    }
    if (c.size() == 0) {
      out.print("There are currently no known " + type + " Task Trackers.");
    } else {
      out.print("<center>\n");
      out.print("<table class=\"datatable\">\n");
      out.print("<thead>\n");
      out.print("<tr><th><b>Name</b></th><th><b>Host</b></th>" +
                "<th><b># running tasks</b></th>" +
                "<th><b>Max Map Tasks</b></th>" +
                "<th><b>Max Reduce Tasks</b></th>" +
                "<th><b>Failures</b></th>" +
                "<th><b>Seconds since heartbeat</b></th></tr>\n");
      out.print("</thead><tbody>");
      int maxFailures = 0;
      String failureKing = null;
      for (Iterator it = c.iterator(); it.hasNext(); ) {
        TaskTrackerStatus tt = (TaskTrackerStatus) it.next();
        long sinceHeartbeat = System.currentTimeMillis() - tt.getLastSeen();
        if (sinceHeartbeat > 0) {
          sinceHeartbeat = sinceHeartbeat / 1000;
        }
        int numCurTasks = 0;
        for (Iterator it2 = tt.getTaskReports().iterator(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
        }
        int numFailures = tt.getFailures();
        if (numFailures > maxFailures) {
          maxFailures = numFailures;
          failureKing = tt.getTrackerName();
        }
        out.print("<tr><td><a href=\"http://");
        out.print(tt.getHost() + ":" + tt.getHttpPort() + "/\">");
        out.print(tt.getTrackerName() + "</a></td><td>");
        out.print(tt.getHost() + "</td><td>" + numCurTasks +
                  "</td><td>" + tt.getMaxMapTasks() +
                  "</td><td>" + tt.getMaxReduceTasks() + 
                  "</td><td>" + numFailures + 
                  "</td><td>" + sinceHeartbeat + "</td></tr>\n");
      }
      out.print("</tbody>\n");
      out.print("</table>\n");
      out.print("</center>\n");
      if (maxFailures > 0) {
        out.print("Highest Failures: " + failureKing + " with " + maxFailures + 
                  " failures<br>\n");
      }
    }
  }

%>

<html>
<head>
<title><%=trackerName%> Hadoop Machine List</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<link rel="icon" type="image/vnd.microsoft.icon" href="/static/images/favicon.ico" />
</head>
<body>
<h1><a href="jobtracker.jsp"><%=trackerName%></a> Hadoop Machine List</h1>

<h2>Task Trackers</h2>
<%
  generateTaskTrackerTable(out, type, tracker);
%>

<%
out.println(ServletUtil.htmlFooter());
%>
