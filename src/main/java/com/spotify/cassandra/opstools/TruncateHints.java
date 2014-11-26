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
