package com.spotify.cassandra.opstools;

import org.apache.cassandra.tools.NodeProbe;

import java.io.IOException;
import java.net.InetAddress;

public class TruncateHints {

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println(String.format("Usage: %s [ALL | host [host ...]]", TruncateHints.class.getName()));
      System.exit(1);
    }

    NodeProbe nodeProbe = new NodeProbe(InetAddress.getLocalHost().getCanonicalHostName(), 7199);

    for (String arg : args) {
      if (arg.equals("ALL"))  {
        nodeProbe.truncateHints();
      } else {
        nodeProbe.truncateHints(arg);
      }
    }

    System.out.println("Hints truncated!");
  }
}
