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
public class Example_16_Table_Weblogs_automap {

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

    // We can automatically map from to a SQL Table as well
    Table weblogsTable = rawWeblogs.map(call(WeblogFromString.class, $("logline")));
    printSchema("Flat SQL Table", weblogsTable);
    weblogsTable.execute().print();
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

    public static Weblog parse(String input) throws Exception {
      // Define regular expression pattern
      String pattern = "^([^ ]*) ([^ ]*) ([^ ]*) \\[(.*?)\\] \"(.*?)\" (\\d{3}) (\\d+) \"(.*?)\" \"(.*?)\"";

      // Use a matcher to extract data
      Matcher matcher = Pattern.compile(pattern).matcher(input);
      matcher.find();

      // Extract and print each field
      Weblog weblog = new Weblog();
      weblog.remoteHost = matcher.group(1);
      weblog.remoteUser = matcher.group(2);
      weblog.remoteUserAuth = matcher.group(3);
      String timestampString = matcher.group(4);
      weblog.requestLine = matcher.group(5);
      weblog.statusCode = Integer.parseInt(matcher.group(6));
      weblog.bytesSent = Integer.parseInt(matcher.group(7));
      weblog.referer = matcher.group(8);
      weblog.userAgent = matcher.group(9);

      // Parse the timestamp
      SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
      dateFormat.setTimeZone(TimeZone.getTimeZone("PST"));
      //weblog.date = dateFormat.parse(timestampString);

      // TODO : Fix this
      weblog.date = timestampString;

      return weblog;
    }
  }
}

