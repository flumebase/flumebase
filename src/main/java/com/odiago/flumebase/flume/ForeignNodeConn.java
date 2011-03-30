/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.flume;

import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Pair;

/**
 * Manages the connection of a foreign node which is listed as a source of data,
 * to the actual flows within this rtsql execution environment.
 *
 * <p>An instance of this class represents a Flume node that is known to the
 * execution environment, and possibly involved in live flows within that
 * environment.</p>
 *
 * <p>When the connect() call is made, the foreign node's destination is
 * modified to use a fan-out of its current sink and an AgentDFOSink. The
 * AgentDFOSink will send data to a logical node running locally within the
 * ExecutionEnvironment.  The local endpoint node will host a CollectorSource
 * and an RtsqlMultiSink--an internally-managed fanout containing a dynamic
 * number of RtsqlSink instances. Each time a flow is added by the user, it
 * results in a new RtsqlSink. Closing a flow removes the corresponding
 * RtsqlSink from this logical node.</p>
 *
 * <p>Closing a ForeignNodeConn will restore the original sink configuration to
 * the node.</p>
 *
 * <p>The EmbeddedFlumeConfig passed to this resource must already be started
 * before attempting to use it. It should be stopped externally after all
 * ForeignNodeConn instances are stopped.</p>
 */
public class ForeignNodeConn {
  private static final Logger LOG = LoggerFactory.getLogger(
      ForeignNodeConn.class.getName());

  // TODO(aaron): Make these next two constants configurable.

  /** Maximum time we wait for Flume to allocate our local node. */
  public static final int MAX_WAIT_TIME_MILLIS = 10000;

  /** Check every 100 milliseconds for an update. */
  public static final int SLEEP_INTERVAL_MILLIS = 100;


  public static final String LOCAL_COLLECTOR_MIN_PORT_KEY = "flumebase.flume.collector.port.min";
  public static final int DEFAULT_MIN_COLLECTOR_PORT = 45000;

  public static final String LOCAL_COLLECTOR_MAX_PORT_KEY = "flumebase.flume.collector.port.max";
  public static final int DEFAULT_MAX_COLLECTOR_PORT = 46000;

  /**
   * Mapping from ports to their (open) ForeignNodeConns. Used to find free ports
   * for new connections.
   */
  private static TreeMap<Integer, ForeignNodeConn> mPortMap =
      new TreeMap<Integer, ForeignNodeConn>();

  /** Name of the Flume node to pull data from. */
  private String mForeignNodeName;

  /** True if we have performed the connect logic. */
  private boolean mIsConnected;
    
  /** The sink configuration of the foreign node before we connected. */
  private String mInitialForeignSink;

  /** The sink configuration we pushed to the foreign node on connect. */
  private String mOpenedForeignSink;

  /** Collector port associated with this foreign node. */
  private int mCollectorPort;

  /** Configuration for this foreign node. */
  private Configuration mConf;

  /** The manager of the embedded Flume node and connections. */
  private EmbeddedFlumeConfig mFlumeConf;

  /** Name of the logical node that receives data from the foreign source. */
  private String mLocalNodeName;

  public ForeignNodeConn(String foreignNodeName, Configuration conf,
      EmbeddedFlumeConfig flumeConf) {
    mForeignNodeName = foreignNodeName;
    mConf = conf;
    mFlumeConf = flumeConf;

    mIsConnected = false;
    mInitialForeignSink = null;
    mOpenedForeignSink = null;
  }

  /** @return true if connect() has returned successfully. */
  public boolean isConnected() {
    return mIsConnected;
  }

  /**
   * Configure the foreign node to forward data to our local collector.
   * Start the local collector node at the same time.
   */
  public void connect() throws IOException {
    if (mIsConnected) {
      throw new IOException("Already connected");
    }

    mCollectorPort = getAvailablePort(this);

    String localSource = "collectorSource(" + mCollectorPort + ")";
    String localSink = "rtsqlmultisink(\"" + mCollectorPort + "\")";

    try {
      // Spawn the local source.
      mLocalNodeName = mForeignNodeName + "-receiver";
      mFlumeConf.spawnLogicalNode(mLocalNodeName, localSource, localSink);

      // Configure the foreign logical node to connect to our node.
      Pair<String, String> curNodeConfig = mFlumeConf.getNodeConfig(mForeignNodeName);
      if (null == curNodeConfig) {
        throw new IOException("No such foreign node: " + mForeignNodeName);
      }

      String foreignSrc = curNodeConfig.getLeft();
      mInitialForeignSink = curNodeConfig.getRight(); // Save this for when we restore.

      // Generate the foreign sink that forwards to our local node.
      // TODO(aaron): Revisit [lack of] reliability guarantee made here.
      String forwardSink = "agentBESink(\"" + mFlumeConf.getHostName() + "\", "
          + mCollectorPort + ")";

      // Check whether the initial foreign sink already contains the text of 'forwardSink'.
      // If so, then we don't need them to send us two copies of the data -- don't
      // reconfigure it!
      if (mInitialForeignSink.indexOf(forwardSink) != -1) {
        // Foreign end is already forwarding!
        mOpenedForeignSink = null;
      } else {
        mOpenedForeignSink = "[" + mInitialForeignSink + ", " + forwardSink + "]";

        // TODO(aaron): This is racy. We want to ensure we're replacing the same configuration
        // we read, otherwise we might conflict with an external reconfig of this node.
        LOG.info("Reconfigured foreign node " + mForeignNodeName + ": " + foreignSrc + " -> "
            + mOpenedForeignSink);
        mFlumeConf.configureLogicalNode(mForeignNodeName, foreignSrc, mOpenedForeignSink);
      }
    } catch (TException te) {
      throw new IOException(te);
    }

    mIsConnected = true;
  }

  /**
   * Close the local collector node and restore the foreign node's configuration
   * to exclude a fanout to the local collector.
   */
  public void close() throws IOException {
    try {
      // Disconnect the foreign node from our local node. If the foreign node
      // has the same sink configuration as the one we installed, replace it
      // with the sink configuration prior to our installation.
      Pair<String, String> curNodeConfig = mFlumeConf.getNodeConfig(mForeignNodeName);
      if (null == curNodeConfig) {
        throw new IOException("No such foreign node: " + mForeignNodeName);
      }

      String foreignSrc = curNodeConfig.getLeft();
      String foreignSink = curNodeConfig.getRight();

      if (null == mOpenedForeignSink) {
        LOG.debug("Did not reconfigure foreign end; no restore required.");
      } else if (null == mInitialForeignSink) {
        LOG.warn("No initial specification to restore for foreign node: " + mForeignNodeName);
      } else if (foreignSink.equals(mOpenedForeignSink)) {
        // Do the restore.
        mFlumeConf.configureLogicalNode(mForeignNodeName, foreignSrc, mInitialForeignSink);
      } else {
        LOG.warn("The sink specification for " + mForeignNodeName
            + " has changed since we connected it to this node! Not replacing current config.");
      }

      LOG.info("Removed connection from Flume node " + mForeignNodeName); 

      // Now close down the local logical node.
      mFlumeConf.decommissionLogicalNode(mLocalNodeName);

    } catch (TException te) {
      throw new IOException(te);
    }

    mIsConnected = false;
  }

  /** @return the id of the RtsqlMultiSink we manage/interface with. */
  protected String getMultiSinkId() {
    return "" + mCollectorPort;
  }

  /**
   * Wait for this ForeignNodeConn's RtsqlMultiSink instance to get created
   * by Flume. (Waits until a timeout)
   *
   * @return true if the sink is available, false otherwise.
   */
  protected boolean waitForMultiSink() {
    long startTime = System.currentTimeMillis();
    long endTime = startTime + MAX_WAIT_TIME_MILLIS;

    String id = getMultiSinkId();

    do {
      RtsqlMultiSink multiSink = RtsqlMultiSink.getMultiSinkInstance(id);
      if (null != multiSink) {
        // It's instantiated.
        return true;
      }

      try {
        Thread.sleep(SLEEP_INTERVAL_MILLIS);
      } catch (InterruptedException ie) {
        return false; // couldn't find it before interrupt.
      }
    } while (System.currentTimeMillis() < endTime);

    // Couldn't find it before timeout.
    return false;
  }

  /**
   * Add a sink to the local endpoint node. This adds a sink for the specified flowSourceId.
   */
  public void addLocalSink(String flowSourceId) throws IOException {
    RtsqlSink rtsqlSink = new RtsqlSink(flowSourceId);

    if (!waitForMultiSink()) {
      throw new IOException("Could not get local RtsqlMultiSink to attach flow");
    }
    RtsqlMultiSink multiSink = RtsqlMultiSink.getMultiSinkInstance(getMultiSinkId());
    if (null == multiSink) {
      LOG.warn("Cannot add flow source " + flowSourceId
          + " to multi sink with id " + getMultiSinkId() + "; no such RtsqlMultiSink");
      return;
    }

    multiSink.addChildSink(flowSourceId, rtsqlSink);
  }

  /**
   * Remove the sink for a given flowSourceId from the local endpoint node.
   */
  public void removeLocalSink(String flowSourceId) throws IOException {
    // TODO(aaron): If there are no local sinks left, should this auto-close?
    // Maybe after a timeout? (Maybe do this in EmbeddedFlumeConfig instead?)
    // If we auto-close, we need to update the map in EmbeddedFlumeConfig.
    RtsqlMultiSink multiSink = RtsqlMultiSink.getMultiSinkInstance(getMultiSinkId());
    if (null == multiSink) {
      LOG.warn("Cannot remove flow source " + flowSourceId
          + " from multi sink with id " + getMultiSinkId() + "; no such RtsqlMultiSink");
      return;
    }

    multiSink.removeChildSink(flowSourceId);
  }

  @Override
  public String toString() {
    return "ForeignNodeConn(" + mForeignNodeName + ")";
  }

  /**
   * Consult mPortMap to determine what port is available in our specified range
   * and return that port number.
   * @throws IOException if it cannot find a free port.
   */
  private static int getAvailablePort(ForeignNodeConn conn) throws IOException {
    int lowPort = conn.mConf.getInt(LOCAL_COLLECTOR_MIN_PORT_KEY, DEFAULT_MIN_COLLECTOR_PORT);
    int hiPort = conn.mConf.getInt(LOCAL_COLLECTOR_MAX_PORT_KEY, DEFAULT_MAX_COLLECTOR_PORT);
    
    synchronized (mPortMap) {
      for (int port = lowPort; port < hiPort; port++) {
        if (mPortMap.containsKey(Integer.valueOf(port))) {
          continue;
        }

        // This port is open -- bind it in our map and return.
        mPortMap.put(Integer.valueOf(port), conn);
        return port;
      }
    }

    // Couldn't find a port in the range. Bail with an error.
    throw new IOException("Could not find free port in range [" + lowPort + ", " + hiPort + ")");
  }
}
