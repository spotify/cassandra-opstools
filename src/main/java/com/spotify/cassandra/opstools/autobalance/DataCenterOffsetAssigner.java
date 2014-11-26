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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataCenterOffsetAssigner {
  private static final int MAX_OFFSETS = 10;

  public int fewestMovesNeeed;
  public Map<String, Integer> bestAssignment;

  private Map<String, Integer> currentAssignment;
  private final List<String> dcs;
  private final boolean[] activeOffset;
  private final Map<String, BigInteger> currentMap;
  private final Map<String, String> hostDc;
  private final BigInteger[] tokens;

  public DataCenterOffsetAssigner(
      Map<String, BigInteger> currentMap,
      Map<String, String> hostDc,
      BigInteger[] tokens) {
    this.currentMap = currentMap;
    this.hostDc = hostDc;
    this.tokens = tokens;
    this.activeOffset = new boolean[10];
    this.dcs = new ArrayList<String>();
  }

  public Map<String, Integer> findBestAssignment() {
    init();

    fewestMovesNeeed = Integer.MAX_VALUE;
    currentAssignment = new HashMap<String, Integer>();
    recurse(0, new boolean[activeOffset.length]);

    if (fewestMovesNeeed == Integer.MAX_VALUE) {
      throw new RuntimeException("No assignment found!?");
    }

    return bestAssignment;
  }

  private void init() {
    // Find which offsets are "active" (already in use by some of the tokens)
    // We want to reuse them if possible

    int noActive = 0;
    for (Map.Entry<String, BigInteger> entry : currentMap.entrySet()) {
      String dc = hostDc.get(entry.getKey());
      if (!dcs.contains(dc)) {
        dcs.add(dc);
      }
      BigInteger token = entry.getValue();
      for (int j = 0; j < tokens.length; j++) {
        BigInteger diff = token.subtract(tokens[j]);
        if (diff.doubleValue() >= 0 && diff.doubleValue() < MAX_OFFSETS) {
          int offset = diff.intValue();
          assert offset >= 0 && offset < MAX_OFFSETS;
          if (!activeOffset[offset]) {
            noActive++;
            activeOffset[offset] = true;
          }
        }
      }
    }

    if (dcs.size() > MAX_OFFSETS) {
      throw new RuntimeException("Too many data centers!");
    }

    // Must be at least as many active offsets as there are dc's
    int i = 0;
    while (noActive < dcs.size()) {
      while (activeOffset[i]) { i++; }
      activeOffset[i++] = true;
      noActive++;
    }
  }

  private void recurse(int curDc, boolean[] offsetsTaken) {
    if (curDc == dcs.size()) {
      int movesNeeded = evaluate(currentAssignment);
      if (movesNeeded < fewestMovesNeeed) {
        fewestMovesNeeed = movesNeeded;
        bestAssignment = new HashMap<String, Integer>(currentAssignment);
      }
    } else {
      for (int i = 0; i < activeOffset.length; i++) {
        if (activeOffset[i] && !offsetsTaken[i]) {
          currentAssignment.put(dcs.get(curDc), i);
          offsetsTaken[i] = true;
          recurse(curDc + 1, offsetsTaken);
          offsetsTaken[i] = false;
          currentAssignment.remove(dcs.get(curDc));
        }
      }
    }
  }

  private int evaluate(Map<String, Integer> currentAssignment) {
    int moves = 0;
    for (Map.Entry<String, BigInteger> entry : currentMap.entrySet()) {
      boolean mustMove = true;
      String dc = hostDc.get(entry.getKey());
      BigInteger offset = BigInteger.valueOf(currentAssignment.get(dc));
      for (BigInteger token : tokens) {
        if (entry.getValue().subtract(token).equals(offset)) {
          mustMove = false;
        }
      }
      if (mustMove) {
        moves++;
      }
    }
    return moves;
  }
}
