/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.spotify.cassandra.opstools.autobalance;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
  private void run(CommandLine cmd) throws IOException, InterruptedException {
    boolean dryrun = cmd.hasOption("d");
    boolean force = cmd.hasOption("f");
    boolean noresolve = cmd.hasOption("r");
    int port = cmd.hasOption("p") ? Integer.parseInt(cmd.getOptionValue("p")) : 7199;
    String nodehost = cmd.hasOption("h") ? cmd.getOptionValue("h") : "localhost";

    System.out.println("Collecting information about the cluster...");

    NodeProbe nodeProbe = new NodeProbe(nodehost, port);

    if (nodeProbe.getTokens().size() != 1) {
      System.err.println("Cluster is using vnodes and should already be automatically balanced!");
      System.exit(1);
    }

    boolean hasData = false;
    if (!dryrun) {
      Map<String, String> loadMap = nodeProbe.getLoadMap();
      for (String s : loadMap.values()) {
        if (s.contains("KB"))
          continue;
        if (s.contains("MB") || s.contains("GB") || s.contains("TB")) {
          hasData = true;
          continue;
        }
        throw new RuntimeException("Unknown suffix in load map; don't dare to continue");
      }
    }

    String partitioner = nodeProbe.getPartitioner();
    BigInteger minToken, maxToken;

    if (partitioner.equals(RandomPartitioner.class.getName())) {
      minToken = RandomPartitioner.ZERO;
      maxToken = RandomPartitioner.MAXIMUM;
    } else if (partitioner.equals(Murmur3Partitioner.class.getName())) {
      minToken = BigInteger.valueOf(Murmur3Partitioner.MINIMUM.token);
      maxToken = BigInteger.valueOf(Murmur3Partitioner.MAXIMUM);
    } else {
      throw new RuntimeException("Unsupported partitioner: " + partitioner);
    }

    // Get current mapping of all live nodes

    List<String> liveNodes = nodeProbe.getLiveNodes();

    Map<String, BigInteger> hostTokenMap = new HashMap<String, BigInteger>();
    Map<String, String> hostDcMap = new HashMap<String, String>();

    for (String host : liveNodes) {
      String dc = nodeProbe.getEndpointSnitchInfoProxy().getDatacenter(host);

      String decoratedHost = host;

      if (!noresolve) {
        // Prefix host with canonical host name.
        // This makes things prettier and also causes tokens to be assigned in logical order.
        decoratedHost = InetAddress.getByName(host).getCanonicalHostName() + "/" + host;
      } else {
        decoratedHost = "/" + host;
      }

      hostDcMap.put(decoratedHost, dc);

      List<String> tokens = nodeProbe.getTokens(host);

      if (tokens.size() > 1) {
        throw new RuntimeException("vnodes not supported");
      }
      if (tokens.size() == 0) {
        throw new RuntimeException("No token for " + host + "; aborting");
      }

      hostTokenMap.put(decoratedHost, new BigInteger(tokens.get(0)));
    }

    Balancer balancer = new Balancer(hostTokenMap, hostDcMap, minToken, maxToken);
    Map<String, BigInteger> newMap = balancer.balance();

    List<Operation> operations = new ArrayList<Operation>();

    boolean movesNeeded = false;
    for (Map.Entry<String, BigInteger> entry : hostTokenMap.entrySet()) {
      String host = entry.getKey();
      BigInteger oldToken = entry.getValue();
      BigInteger newToken = newMap.get(host);
      if (!oldToken.equals(newToken)) {
        movesNeeded = true;
      }
      operations.add(new Operation(host, hostDcMap.get(host), oldToken, newToken));
    }

    if (movesNeeded && hasData && !dryrun && !force) {
      dryrun = true;
      System.out.println("The cluster is unbalanced but has data, so no operations will actually be carried out. Use --force if you want the cluster to balance anyway.");
    }

    Collections.sort(operations);

    boolean unbalanced = false, moved = false;
    for (Operation op : operations) {
      if (op.oldToken.equals(op.newToken)) {
        System.out.println(op.host + ": Stays on token " + op.oldToken);
      } else {
        System.out.println(op.host + ": Moving from token " + op.oldToken + " to token " + op.newToken);
        if (!dryrun) {
          String ip = op.host.substring(op.host.lastIndexOf("/") + 1);
          NodeProbe np = new NodeProbe(ip, 7199);
          np.move(op.newToken.toString());
          moved = true;
        } else {
          unbalanced = true;
        }
      }
    }

    if (!unbalanced && moved) {
      System.out.println("The cluster is now balanced!");
    }
  }

  private static class Operation implements Comparable<Operation> {
    public String host;
    public String dataCenter;
    public BigInteger oldToken;
    public BigInteger newToken;

    private Operation(String host, String dataCenter, BigInteger oldToken, BigInteger newToken) {
      this.host = host;
      this.dataCenter = dataCenter;
      this.oldToken = oldToken;
      this.newToken = newToken;
    }

    @Override
    public int compareTo(Operation o) {
      if (!dataCenter.equals(o.dataCenter)) {
        return dataCenter.compareTo(o.dataCenter);
      }

      return newToken.compareTo(o.newToken);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, ParseException {
    final Options options = new Options();
    options.addOption("f", "force", false, "Force auto balance");
    options.addOption("d", "dryrun", false, "Dry run");
    options.addOption("r", "noresolve", false, "Don't resolve host names");
    options.addOption("h", "host", true, "Host to connect to (default: localhost)");
    options.addOption("p", "port", true, "Port to connect to (default: 7199)");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    new Main().run(cmd);
  }
}
