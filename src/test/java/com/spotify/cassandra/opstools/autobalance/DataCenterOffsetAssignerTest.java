package com.spotify.cassandra.opstools.autobalance;

import com.google.common.collect.ImmutableMap;

import com.spotify.cassandra.opstools.autobalance.DataCenterOffsetAssigner;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Map;

public class DataCenterOffsetAssignerTest {

  @Test
  public void singleDcOneMove() {
    DataCenterOffsetAssigner assigner = new DataCenterOffsetAssigner(
        ImmutableMap.of("a", token(0), "b", token(1001), "c", token(2001)),
        ImmutableMap.of("a", "cloud", "b", "cloud", "c", "cloud"),
        new BigInteger[] { token(0), token(1000), token(2000) }
    );

    Map<String, Integer> best = assigner.findBestAssignment();

    Assert.assertEquals(1, assigner.fewestMovesNeeed);
    Assert.assertEquals(1, (int) best.get("cloud"));
  }

  @Test
  public void singleDcAllMoves() {
    DataCenterOffsetAssigner assigner = new DataCenterOffsetAssigner(
        ImmutableMap.of("a", token(534), "b", token(2512), "c", token(2983)),
        ImmutableMap.of("a", "cloud", "b", "cloud", "c", "cloud"),
        new BigInteger[] { token(0), token(1000), token(2000) }
    );

    Map<String, Integer> best = assigner.findBestAssignment();

    Assert.assertEquals(3, assigner.fewestMovesNeeed);
    Assert.assertEquals(0, (int) best.get("cloud"));
  }

  @Test
  public void expandMultiDc() {
    // Corresponds to a case where we have a cluster containing 1 node in 2 DC's that we expand to 2 nodes in each DC

    DataCenterOffsetAssigner assigner = new DataCenterOffsetAssigner(
        ImmutableMap.of(
            "dc1-old", token(5), "dc1-new", token(512),
            "dc2-old", token(9), "dc2-new", token(1870)),
        ImmutableMap.of("dc1-old", "dc1", "dc2-old", "dc2", "dc1-new", "dc1", "dc2-new", "dc2"),
        new BigInteger[] { token(0), token(1000) }
    );

    Map<String, Integer> best = assigner.findBestAssignment();

    Assert.assertEquals(2, assigner.fewestMovesNeeed);
    Assert.assertEquals(5, (int) best.get("dc1"));
    Assert.assertEquals(9, (int) best.get("dc2"));
  }

  @Test
  public void setupMultiDc() {
    // Corresponds to a case where we create a cluster with 3 DC's, two nodes each.
    // Some nodes accidentally map to some offsets

    DataCenterOffsetAssigner assigner = new DataCenterOffsetAssigner(
        new ImmutableMap.Builder<String, BigInteger>()
            .put("dc1-a", token(735))
            .put("dc1-b", token(7))     // offset 7
            .put("dc2-c", token(1001))  // offset 1
            .put("dc2-d", token(1008))  // offset 8
            .put("dc3-e", token(100))
            .put("dc3-f", token(1))     // offset 1
            .build(),
        new ImmutableMap.Builder<String, String>()
            .put("dc1-a", "dc1")
            .put("dc1-b", "dc1")
            .put("dc2-c", "dc2")
            .put("dc2-d", "dc2")
            .put("dc3-e", "dc3")
            .put("dc3-f", "dc3")
            .build(),
        new BigInteger[] { token(0), token(1000) }
    );

    Map<String, Integer> best = assigner.findBestAssignment();

    Assert.assertEquals(3, assigner.fewestMovesNeeed);
    Assert.assertEquals(7, (int) best.get("dc1"));
    Assert.assertEquals(8, (int) best.get("dc2"));
    Assert.assertEquals(1, (int) best.get("dc3"));
  }

  private static BigInteger token(int token) {
    return BigInteger.valueOf(token);
  }
}
