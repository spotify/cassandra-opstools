package com.spotify.cassandra.opstools;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import org.apache.cassandra.locator.DynamicEndpointSnitchMBean;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DynamicSnitchDumper {
  private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
  private static final String dsnitchObjName = "org.apache.cassandra.db:type=DynamicEndpointSnitch";

  public static void main(String ... args) throws IOException, MalformedObjectNameException {

    String host = "localhost";
    if (args.length > 0)
      host = args[0];
    int port = 7199;
    if (args.length > 1)
      port = Integer.parseInt(args[1]);
    boolean munin = false;
    if (args.length > 2 && args[2].equals("munin"))
      munin = true;
    boolean muninConfig = false;
    if (args.length > 3 && args[3].equals("config"))
      muninConfig = true;

    JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
    Map<String,Object> env = new HashMap<String,Object>();
    DynamicEndpointSnitchMBean dsnitch = getDSnitchMbean(jmxUrl, env);

    Map<InetAddress, Double> sorted = sortMap(dsnitch.getScores());
    if (munin) {
      if (muninConfig)
        printMuninConfig(sorted);
      else
        printForMunin(sorted);
    } else {
      for(Map.Entry<InetAddress, Double> score : sorted.entrySet()) {
        System.out.println(score.getKey().getHostName() + " : "+score.getValue());
      }
    }
  }
  private static String getMuninHost(String hostname) {
    return hostname.replace(".spotify.net", "").replace(".","_");
  }
  private static void printForMunin(Map<InetAddress, Double> sorted) {
    for(Map.Entry<InetAddress, Double> score : sorted.entrySet()) {
      String hostName = getMuninHost(score.getKey().getHostName());
      System.out.println(hostName+".value "+score.getValue());
    }
  }

  private static void printMuninConfig(Map<InetAddress, Double> sorted) {
    System.out.println("graph_category Cassandra");
    System.out.println("graph_title DynamicSnitch scores");
    System.out.println("graph_vlabel score");
    for(Map.Entry<InetAddress, Double> score : sorted.entrySet()) {
      String hostName = getMuninHost(score.getKey().getHostName());
      System.out.println(hostName+".graph yes");
      System.out.println(hostName+".label "+hostName);
    }
  }

  private static DynamicEndpointSnitchMBean getDSnitchMbean(JMXServiceURL jmxUrl, Map<String, Object> env) throws IOException, MalformedObjectNameException {
    JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, env);
    MBeanServerConnection mbeanServerConn = jmxc.getMBeanServerConnection();
    Set<ObjectName> objs = mbeanServerConn.queryNames(null, null);
    ObjectName realName = null;

    // in cassandra 1.1, the mbean has a "random" instance number, listing mbeans and finding the real one.
    for (ObjectName ob : objs) {
      if (ob.getCanonicalName().contains("DynamicEndpointSnitch"))
        realName = ob;
    }
    if (realName != null)
      return JMX.newMBeanProxy(mbeanServerConn, realName, DynamicEndpointSnitchMBean.class);
    else
      throw new RuntimeException("Could not find the DynamicEndpointSnitch mbean!");
  }

  private static Map<InetAddress, Double> sortMap(Map<InetAddress, Double> scores) {
    return ImmutableSortedMap.copyOf(scores, Ordering.natural().onResultOf(Functions.forMap(scores)).compound(new Comparator<InetAddress>() {
          @Override
          public int compare(InetAddress o1, InetAddress o2) {
            return o1.toString().compareTo(o2.toString());
          }

          @Override
          public boolean equals(Object obj) {
            return false;
          }
        }));
  }
}
