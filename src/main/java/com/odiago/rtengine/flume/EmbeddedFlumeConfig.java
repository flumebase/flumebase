// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Client;
import com.cloudera.flume.conf.thrift.FlumeMasterCommandThrift;
import com.cloudera.flume.master.FlumeMaster;

/**
 * Manages the configuration of embedded Flume components within the
 * current process.
 */
public class EmbeddedFlumeConfig {
  private static final Logger LOG = LoggerFactory.getLogger(
      EmbeddedFlumeConfig.class.getName());

  /** Configuration key specifying the root of your Flume installation. */
  public static final String FLUME_HOME_KEY = "flume.home";

  /** Configuration key specifying the config dir for your Flume installation. */
  public static final String FLUME_CONF_DIR_KEY = "flume.conf.dir";

  /**
   * Configuration key set to true if a Flume master node should be embedded in
   * the current process. This provides us with a fully standalone rtengine process.
   */
  public static final String EMBED_FLUME_MASTER_KEY = "embedded.flume.master";
  public static final boolean DEFAULT_EMBED_FLUME_MASTER = true;

  /** Configuration property specifying the (external) FlumeMaster to connect to. */
  public static final String FLUME_MASTER_HOST_KEY = "rtengine.flume.master.host";
  public static final String DEFAULT_FLUME_MASTER_HOST = "localhost";
  
  /** Configuration property specifying the (external) FlumeMaster's port. */
  public static final String FLUME_MASTER_PORT_KEY = "rtengine.flume.master.port";
  public static final int DEFAULT_FLUME_MASTER_PORT = 35873;

  /** The complete set of Flume nodes spawned for our application. */
  private Collection<FlumeNode> mFlumeNodes;

  /** Thrift client to communicate with the master node. */
  private Client mMasterClient;

  /** If we are running in standalone mode, the FlumeMaster spawned
   * within our application.
   */
  private FlumeMaster mFlumeMaster;

  /** The FlumeConfiguration instance to use within FlumeMaster, FlumeNode, etc. */
  private FlumeConfiguration mFlumeConf;

  /** The standard configuration used to initialize our application. */
  private Configuration mConf;

  /** Hostname which corresponds to the physical node name we'll start,
   * and which is a component of all the logical nodes we spawn on it. */
  private String mHostName;

  public EmbeddedFlumeConfig(Configuration conf) {
    mConf = conf;
    mFlumeNodes = new LinkedList<FlumeNode>();
    mHostName = getHostName();
  }
  
  /**
   * @return a FlumeConfiguration instance that is populated with settings
   * required by logreceiver.
   *
   * Don't call this until flume.home/flume.conf.dir have been set appropriately.
   */
  private FlumeConfiguration getFlumeConf() {
    FlumeConfiguration conf = FlumeConfiguration.get();

    // Ensure that our FlumePlugin class is at the head of the registry.
    String existingPlugins = conf.getPluginClasses();
    String newPlugins = FlumePlugin.class.getName();
    if (null != existingPlugins && existingPlugins.length() > 0) {
      newPlugins = newPlugins + "," + existingPlugins;
    }
    conf.set(FlumeConfiguration.PLUGIN_CLASSES, newPlugins);
    
    return conf;
  }

  /**
   * @return true if we're running a fully standalone flume deployment
   * inside this application.
   */
  public boolean isStandalone() {
    return mConf.getBoolean(EMBED_FLUME_MASTER_KEY, DEFAULT_EMBED_FLUME_MASTER);
  }

  /** Starts Flume services. */
  public void start() throws IOException {
    LOG.debug("Starting embedded Flume service");

    // If we've configured within this application the means to find Flume,
    // load this into the global properties before acquiring a Flume config.
    String flumeHome = mConf.get(FLUME_HOME_KEY);
    String flumeConfDir = mConf.get(FLUME_CONF_DIR_KEY);
    if (null != flumeHome) {
      System.setProperty("flume.home", flumeHome);
      LOG.debug("Setting flume.home=" + flumeHome);
    }

    if (null != flumeConfDir) {
      System.setProperty("flume.conf.dir", flumeConfDir);
      LOG.debug("Setting flume.conf.dir=" + flumeConfDir);
    }

    mFlumeConf = getFlumeConf();
    if (isStandalone()) {
      // If we need to start a master, do so before launching any nodes.
      startMaster();
    }

    try {
      connectToMaster();
    } catch (TTransportException tte) {
      throw new IOException("Could not connect to Flume master", tte);
    }

    startPhysicalNode();
    LOG.debug("Physical node is ready");
  }

  /**
   * Starts a Flume PhysicalNode that manages our data flows.
   */
  private void startPhysicalNode() {
    String nodeName = mHostName;
    boolean startHttp = false;
    boolean isOneShot = false;

    FlumeNode node = new FlumeNode(nodeName, mFlumeConf, startHttp, isOneShot);
    node.start();
    addFlumeNode(node);
  }

  /**
   * Creates an EventSink that forwards data to the specified flowSourceId;
   * instruct the flume master to bind this EventSink to our internal
   * logical node.
   * @param flowSourceId - the identifier for the RTSQL flow/source being
   * populated by this logical node.
   * @param sourceStr - the Flume EventSource operating on this node.
   * Can be a collectorSource() to read from an upstream Flume RPC network, or
   * a specific file- or other resource-based source.
   */
  public void createFlowSink(String flowSourceId, String sourceStr)
      throws TException {
    String nodeName = flowSourceId;
    String sinkStr = "rtsqlsink(\"" + flowSourceId + "\")";

    spawnLogicalNode(nodeName, sourceStr, sinkStr);
  }

  /**
   * Removes the logical node for the specified flowSourceId from Flume.
   */
  public void stopFlowSink(String flowSourceId) throws TException {
    List<String> args = new ArrayList<String>();
    args.add(flowSourceId);
    FlumeMasterCommandThrift cmd = new FlumeMasterCommandThrift("decommission", args);
    mMasterClient.submit(cmd);
  }

  private void spawnLogicalNode(String logicalNode, String source, String sink)
      throws TException {

    // Configure the logical node to use our rtsqlsink and the user's source.
    List<String> args = new ArrayList<String>();
    args.add(logicalNode);
    args.add(source);
    args.add(sink);
    FlumeMasterCommandThrift cmd = new FlumeMasterCommandThrift("config", args);
    mMasterClient.submit(cmd);

    // Spawn the logical node on our local physical node.
    args.clear();
    args.add(mHostName);
    args.add(logicalNode);
    cmd = new FlumeMasterCommandThrift("spawn", args);
    mMasterClient.submit(cmd);
  }


  /**
   * Starts an embedded instance of the FlumeMaster service.  Used when
   * rtengine is running in a "standalone" mode.
   */
  private void startMaster() {
    LOG.info("Starting standalone Flume master.");
    mFlumeConf.set("flume.master.store", "memory");
    FlumeMaster master = new FlumeMaster(mFlumeConf, false);
    try {
      master.serve();
      mFlumeMaster = master;
    } catch (IOException ioe) {
      LOG.error("IOException starting Flume master: " + ioe);
    }
  }

  /**
   * Stop our Flume nodes.
   */
  public void stop() {
    LOG.info("Stopping Flume nodes...");
    for (FlumeNode flumeNode : mFlumeNodes) {
      flumeNode.stop();
    }

    if (null != mFlumeMaster) {
      LOG.info("Stopping Flume master");
      mFlumeMaster.shutdown();
    }

    mMasterClient = null;
    mFlumeNodes.clear();
    mFlumeMaster = null;
  }

  /**
   * Adds a FlumeNode to the set of nodes that we have started for this app.
   * The set can be later retrieved and manipulated, stopped, etc.
   */
  private void addFlumeNode(FlumeNode node) {
    mFlumeNodes.add(node);
  }

  /**
   * @return the local hostname.
   */
  private String getHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      LOG.warn("Could not determine local hostname: " + uhe);
      return "localhost";
    }
  }

  /**
   * Connect to an external FlumeMaster and return the Thrift client
   * binding.
   */
  private void connectToMaster() throws TTransportException {
    String host;
    int port;

    if (isStandalone()) {
      host = DEFAULT_FLUME_MASTER_HOST;
      port = DEFAULT_FLUME_MASTER_PORT;
    } else {
      host = mConf.get(FLUME_MASTER_HOST_KEY, DEFAULT_FLUME_MASTER_HOST);
      port = mConf.getInt(FLUME_MASTER_PORT_KEY, DEFAULT_FLUME_MASTER_PORT);
    }
    
    TTransport masterTransport = new TSocket(host, port);
    TProtocol protocol = new TBinaryProtocol(masterTransport);
    masterTransport.open();
    mMasterClient = new Client(protocol);
  }
}
