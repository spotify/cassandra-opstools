package com.spotify.cassandra.opstools.autobalance;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Map;

public class BalancerTest {
  @Test
  public void singleDcSetup() {
    Balancer balancer = new Balancer(
        ImmutableMap.of("b", token(120), "a", token(430), "c", token(1020)),
        ImmutableMap.of("a", "cloud", "b", "cloud", "c", "cloud"),
        token(0), token(3000));

    Map<String, BigInteger> best = balancer.balance();

    Assert.assertEquals(token(0), best.get("a"));
    Assert.assertEquals(token(1000), best.get("b"));
    Assert.assertEquals(token(2000), best.get("c"));
  }

  @Test
  public void negativeTokens() {
    Balancer balancer = new Balancer(
        ImmutableMap.of("b", token(120), "a", token(430), "c", token(1020)),
        ImmutableMap.of("a", "cloud", "b", "cloud", "c", "cloud"),
        token(-100), token(200));

    Map<String, BigInteger> best = balancer.balance();

    Assert.assertEquals(token(-100), best.get("a"));
    Assert.assertEquals(token(0), best.get("b"));
    Assert.assertEquals(token(100), best.get("c"));
  }


  @Test
  public void singleDcSetupOneMatches() {
    Balancer balancer = new Balancer(
        ImmutableMap.of("a", token(430), "b", token(120), "c", token(1005)),
        ImmutableMap.of("a", "cloud", "b", "cloud", "c", "cloud"),
        token(0), token(3000));

    Map<String, BigInteger> best = balancer.balance();

    Assert.assertEquals(token(5), best.get("a"));
    Assert.assertEquals(token(2005), best.get("b"));
    Assert.assertEquals(token(1005), best.get("c"));
  }

  @Test
  public void setupMultiDc() {
    // Corresponds to a case where we create a cluster with 3 DC's, two nodes each.
    // Some nodes accidentally map to some offsets

    Balancer balancer = new Balancer(
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
        token(0), token(2000)
    );

    Map<String, BigInteger> best = balancer.balance();

    Assert.assertEquals(token(1007), best.get("dc1-a"));
    Assert.assertEquals(token(7), best.get("dc1-b"));
    Assert.assertEquals(token(8), best.get("dc2-c"));
    Assert.assertEquals(token(1008), best.get("dc2-d"));
    Assert.assertEquals(token(1001), best.get("dc3-e"));
    Assert.assertEquals(token(1), best.get("dc3-f"));
  }

  private static BigInteger token(int token) {
    return BigInteger.valueOf(token);
  }
}
