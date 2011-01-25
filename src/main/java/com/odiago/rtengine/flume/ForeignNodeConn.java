// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Pair;

import com.odiago.rtengine.util.StringUtils;

/**
 * Manages the connection of a foreign node which is listed as a source of data,
 * to the actual flows within this rtsql execution environment.
 *
 * <p>An instance of this class represents a Flume node that is known to the
 * execution environment, and possibly involved in live flows within that
 * environment.</p>
 *
 * <p>When the connect() call is made, the foreign node's destination is modified
 * to use a fan-out of its current sink and an AgentDFOSink. The AgentDFOSink will
 * send data to a logical node running locally within the ExecutionEnvironment.
 * The local endpoint node will host a CollectorSource and a fanout containing
 * a dynamic number of RtsqlSink instances. Each time a flow is added by the user,
 * it results in a new RtsqlSink. Closing a flow removes the corresponding RtsqlSink
 * from this logical node.</p>
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

  public static final String LOCAL_COLLECTOR_MIN_PORT_KEY = "rtengine.flume.collector.port.min";
  public static final int DEFAULT_MIN_COLLECTOR_PORT = 45000;

  public static final String LOCAL_COLLECTOR_MAX_PORT_KEY = "rtengine.flume.collector.port.max";
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

  /** List of sinks in the fan-out associated with the local receiver node. */ 
  private List<String> mLocalSinks; 

  /** The manager of the embedded Flume node and connections. */
  private EmbeddedFlumeConfig mFlumeConf;

  /** Name of the logical node that receives data from the foreign source. */
  private String mLocalNodeName;

  public ForeignNodeConn(String foreignNodeName, Configuration conf,
      EmbeddedFlumeConfig flumeConf) {
    mForeignNodeName = foreignNodeName;
    mConf = conf;
    mFlumeConf = flumeConf;

    mLocalSinks = new ArrayList<String>();
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
    mLocalSinks.clear();
    mLocalSinks.add("null");

    // Format mLocalSinks into a stringified list for fanout.
    String localSinks = formatFanoutSinks(mLocalSinks);

    try {
      // Spawn the local source.
      mLocalNodeName = mForeignNodeName + "-receiver";
      mFlumeConf.spawnLogicalNode(mLocalNodeName, localSource, localSinks);

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
      mOpenedForeignSink = "[" + mInitialForeignSink + ", " + forwardSink + "]";

      // TODO(aaron): This is racy. We want to ensure we're replacing the same configuration
      // we read, otherwise we might conflict with an external reconfig of this node.
      LOG.info("Reconfigured foreign node " + mForeignNodeName + ": " + foreignSrc + " -> "
          + mOpenedForeignSink);
      mFlumeConf.configureLogicalNode(mForeignNodeName, foreignSrc, mOpenedForeignSink);
    } catch (TException te) {
      throw new IOException(te);
    }

    mIsConnected = true;
  }

  /**
   * Given a set of sink specifications, return the specification for a fanout
   * sink that encompasses all of them.
   */
  private String formatFanoutSinks(List<String> sinks) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    StringUtils.formatList(sb, sinks);
    sb.append("]");
    return sb.toString();
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

      if (null == mInitialForeignSink) {
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

  /**
   * Add a sink to the local endpoint node. This adds a sink for the specified flowSourceId.
   */
  public void addLocalSink(String flowSourceId) throws IOException {
    String sinkStr = "rtsqlsink(\"" + flowSourceId + "\")";
    mLocalSinks.add(sinkStr);
    reconfigureLocalNode();
  }

  /**
   * Remove the sink for a given flowSourceId from the local endpoint node.
   */
  public void removeLocalSink(String flowSourceId) throws IOException {
    // TODO(aaron): If there are no local sinks left, should this auto-close?
    // Maybe after a timeout? (Maybe do this in EmbeddedFlumeConfig instead?)
    // If we auto-close, we need to update the map in EmbeddedFlumeConfig.
    String sinkStr = "rtsqlsink(\"" + flowSourceId + "\")";
    mLocalSinks.remove(sinkStr);
    reconfigureLocalNode();
  }

  /**
   * Update the configuration of the local endpoint node to reflect the current set of sinks.
   */
  private void reconfigureLocalNode() throws IOException {
    String localSource = "collectorSource(" + mCollectorPort + ")";
    String localSinks = formatFanoutSinks(mLocalSinks);
    try {
      mFlumeConf.configureLogicalNode(mLocalNodeName, localSource, localSinks);
    } catch (TException te) {
      throw new IOException(te);
    }
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
