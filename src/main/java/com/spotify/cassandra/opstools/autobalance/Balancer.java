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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Balancer {

  private final Map<String, BigInteger> currentMap;
  private final Map<String, String> hostDc;
  private final BigInteger minToken;
  private final BigInteger maxToken;

  private int dcSize;

  public Balancer(Map<String, BigInteger> currentMap, Map<String, String> hostDc, BigInteger minToken, BigInteger maxToken) {
    this.currentMap = currentMap;
    this.hostDc = hostDc;
    this.minToken = minToken;
    this.maxToken = maxToken;
  }

  public Map<String, BigInteger> balance() {
    init();

    // Decide which node in the cluster show have which token
    // We want to do this so the number of token changes are as few as possible
    // Tokens must be unique and evenly distributed within datacenters
    // and the "first" node in each DC should have the "same" token, differing only by an offset
    //
    // Example: if min/max is 0/3000 and we have 2 datacenters with 3 nodes each, the tokens to distribute will be 0, 1, 1000, 1001, 2000, 2001
    // The offset for one dc is 0 and the other dc is 1.

    BigInteger[] tokens = new BigInteger[dcSize];
    for (int i = 0; i < dcSize; i++) {
      tokens[i] = (maxToken.subtract(minToken)).multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(dcSize)).add(minToken);
    }

    // First figure out which offset each DC should have, based on current tokens
    // Try assign all offsets (that makes sense) to all DC's - this is exponential, but number of DC's is very small.
    // For each assignment, check how many tokens must be moved. Pick the least amount.

    HashMap<String, BigInteger> newHostTokenMap = new HashMap<String, BigInteger>();
    Map<String, Integer> dcOffsetMap = new DataCenterOffsetAssigner(currentMap, hostDc, tokens).findBestAssignment();

    // For each data center, see which nodes already have a good token
    for (String dc : dcOffsetMap.keySet()) {
      // Calculate tokens for this DC
      Set<BigInteger> dcTokens = new TreeSet<BigInteger>(); // Ensures its sorted
      for (int i = 0; i < tokens.length; i++) {
        dcTokens.add(tokens[i].add(BigInteger.valueOf(dcOffsetMap.get(dc))));
      }
      ArrayList<String> needsToken = new ArrayList<String>();
      for (Map.Entry<String, BigInteger> entry : currentMap.entrySet()) {
        if (hostDc.get(entry.getKey()).equals(dc)) {
          if (dcTokens.contains(entry.getValue())) {
            dcTokens.remove(entry.getValue());
            newHostTokenMap.put(entry.getKey(), entry.getValue());
          } else {
            needsToken.add(entry.getKey());
          }
        }
      }
      assert dcTokens.size() == needsToken.size();

      // Ensures tokens are assigned in order of hostnames
      Collections.sort(needsToken);

      for (String host : needsToken) {
        BigInteger token = dcTokens.iterator().next();
        newHostTokenMap.put(host, token);
        dcTokens.remove(token);
      }
    }
    return newHostTokenMap;
  }

  private void init() {
    Map<String, Integer> dcCount = new HashMap<String, Integer>();

    for (String host : currentMap.keySet()) {
      String dc = hostDc.get(host);

      if (dcCount.containsKey(dc)) {
        dcCount.put(dc, dcCount.get(dc) + 1);
      } else {
        dcCount.put(dc, 1);
      }
    }

    // Check we have same number of nodes in all DC's
    dcSize = -1;
    for (Integer size : dcCount.values()) {
      if (dcSize < 0) {
        dcSize = size;
      } else if (dcSize != size) {
        throw new RuntimeException("Not all datacenters contain the same number of live nodes; aborting");
      }
    }
  }
}
