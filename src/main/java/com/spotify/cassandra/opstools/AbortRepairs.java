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

import org.apache.cassandra.service.StorageServiceMBean;

import java.io.IOException;
import java.net.InetAddress;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class AbortRepairs {

  public static void main(String[] args) throws IOException, InterruptedException {
    JMXServiceURL jmxUrl;
    JMXConnector jmxc = null;
    try {
      jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", InetAddress.getLocalHost().getCanonicalHostName(), 7199));
      jmxc = JMXConnectorFactory.connect(jmxUrl);
      MBeanServerConnection mbeanServerConn = jmxc.getMBeanServerConnection();
      ObjectName name = new ObjectName("org.apache.cassandra.db:type=StorageService");
      StorageServiceMBean ssProxy = JMX.newMBeanProxy(mbeanServerConn, name, StorageServiceMBean.class);

      ssProxy.forceTerminateAllRepairSessions();

      System.out.println("All repair sessions terminated");
    } catch (Exception e) {
      System.err.println("Failed to stop all repair sessions: " + e);
    } finally {
      if (jmxc != null) {
        jmxc.close();
      }
    }
  }
}
