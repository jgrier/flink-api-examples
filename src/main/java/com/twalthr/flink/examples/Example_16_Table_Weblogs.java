package com.twalthr.flink.examples;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Example for dealing with Plain-old Java Objects (POJOs) instead of rows.
 */
public class Example_16_Table_Weblogs {

  public static void main(String[] args) {
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());

    // Read raw strings from Kafka
    Table rawWeblogs = env.fromValues(
      Row.of("server0.example.com - - [01/Mar/2024:14:13:00 -0800] \"GET /index0.html HTTP/1.1\" 200 2000 \"https://www.example.com/referer\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36\"\n"),
      Row.of("server1.example.com - - [01/Mar/2024:14:13:01 -0800] \"GET /index1.html HTTP/1.1\" 200 2001 \"https://www.example.com/referer\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36\"\n"),
      Row.of("server2.example.com - - [01/Mar/2024:14:13:02 -0800] \"GET /index2.html HTTP/1.1\" 200 2002 \"https://www.example.com/referer\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36\"\n"),
      Row.of("server3.example.com - - [01/Mar/2024:14:13:03 -0800] \"GET /index3.html HTTP/1.1\" 200 2003 \"https://www.example.com/referer\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36\"\n"),
      Row.of("server4.example.com - - [01/Mar/2024:14:13:04 -0800] \"GET /index4.html HTTP/1.1\" 200 2004 \"https://www.example.com/referer\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36\"\n")
    ).as("logline");

    printSchema("Raw Log Lines", rawWeblogs);
    rawWeblogs.execute().print();

    // Parse raw weblogs and Map to structured data, here you get a table where there is one column and it's a POJO
    Table structuredWeblogs = rawWeblogs.select(call(WeblogFromString.class, $("logline"))).as("object");
    printSchema("Structured/POJO", structuredWeblogs);
    structuredWeblogs.execute().print();

    // Execute SQL over structured data, without flattening it.
    env.createTemporaryView("structured_weblogs", structuredWeblogs);
    env.sqlQuery("SELECT sum(object.bytesSent) as total FROM structured_weblogs").execute().print();

    // Here I'm filtering the data based on a sub-field of the POJO.
    // This demonstrates working with pojo/structured data without flattening it
    Table filtered = structuredWeblogs.filter($("object").get("bytesSent").isGreater(2001));
    filtered.execute().print();

    // Flatten out into a tabular SQL table
    Table weblogsTable = env.sqlQuery("SELECT object.* FROM structured_weblogs");
    printSchema("Flat SQL Table", weblogsTable);
    weblogsTable.execute().print();

    // Run some more SQL over this tabular data
    env.createTemporaryView("weblogs", weblogsTable);
    Table selected = env.sqlQuery("SELECT * FROM weblogs WHERE remoteHost = 'server4.example.com'");
    selected.execute().print();
  }

  public static void printSchema(String desc, Table t) {
    System.out.println();
    System.out.println("-------------- " + desc + " Schema ----------------------");
    t.printSchema();
    System.out.println();
  }

  // Constructor function for Weblog POJO type
  public static class WeblogFromString extends ScalarFunction {
    public Weblog eval(String rawWeblog) throws Exception {
      return Weblog.parse(rawWeblog);
    }
  }

  public static class Weblog {
    public String remoteHost;
    public String remoteUser;
    public String remoteUserAuth;

    // TODO: Change this to a SQL date
    public String date;
    public String requestLine;
    public int statusCode;
    public int bytesSent;
    public String referer;
    public String userAgent;

    public Weblog(String remoteHost, String remoteUser, String remoteUserAuth, String date, String requestLine, int statusCode, int bytesSent, String referer, String userAgent) {
      this.remoteHost = remoteHost;
      this.remoteUser = remoteUser;
      this.remoteUserAuth = remoteUserAuth;
      this.date = date;
      this.requestLine = requestLine;
      this.statusCode = statusCode;
      this.bytesSent = bytesSent;
      this.referer = referer;
      this.userAgent = userAgent;
    }

    public static Weblog parse(String input) throws Exception {
      // Define regular expression pattern
      String pattern = "^([^ ]*) ([^ ]*) ([^ ]*) \\[(.*?)\\] \"(.*?)\" (\\d{3}) (\\d+) \"(.*?)\" \"(.*?)\"";

      // Use a matcher to extract data
      Matcher matcher = Pattern.compile(pattern).matcher(input);
      matcher.find();

      // Extract and print each field
      String remoteHost = matcher.group(1);
      String remoteUser = matcher.group(2);
      String remoteUserAuth = matcher.group(3);
      String timestampString = matcher.group(4);
      String requestLine = matcher.group(5);
      int statusCode = Integer.parseInt(matcher.group(6));
      int bytesSent = Integer.parseInt(matcher.group(7));
      String referer = matcher.group(8);
      String userAgent = matcher.group(9);

      // Parse the timestamp
      SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
      dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
      //weblog.date = dateFormat.parse(timestampString);

      // TODO : Fix this
      String date = timestampString;

      return new Weblog(remoteHost, remoteUser, remoteUserAuth, date, requestLine, statusCode, bytesSent, referer, userAgent);
    }
  }
}

