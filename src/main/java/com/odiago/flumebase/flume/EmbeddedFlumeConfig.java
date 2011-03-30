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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.master.FlumeMaster;

import com.cloudera.util.Pair;

import com.odiago.flumebase.util.NetUtils;
import com.odiago.flumebase.util.StringUtils;

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
   * the current process. This provides us with a fully standalone flumebase process.
   */
  public static final String EMBED_FLUME_MASTER_KEY = "embedded.flume.master";
  public static final boolean DEFAULT_EMBED_FLUME_MASTER = true;

  /** Configuration property specifying the (external) FlumeMaster to connect to. */
  public static final String FLUME_MASTER_HOST_KEY = "flumebase.flume.master.host";
  public static final String DEFAULT_FLUME_MASTER_HOST = "localhost";
  
  /** Configuration property specifying the (external) FlumeMaster's port. */
  public static final String FLUME_MASTER_PORT_KEY = "flumebase.flume.master.port";
  public static final int DEFAULT_FLUME_MASTER_PORT = 35873;

  /** The complete set of (physical) Flume nodes spawned for our application. */
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

  /** Name of the physical node we manage. */
  private String mPhysicalNodeName;

  /** True after start() has been called. */
  private boolean mIsRunning;

  /**
   * Set of connections to external logical nodes acting as stream sources,
   * keyed by logical node name.
   */
  private Map<String, ForeignNodeConn> mForeignNodeConnections;

  /**
   * Set of logical nodes created by OutputElements; these are physically hosted
   * in our own process, and have an RtsqlMultiSink as their sink; the
   * RtsqlMultiSink's id matches the logical node name. When we are trying
   * to open a ForeignNodeConn, we'll consult this set first and use a
   * LocalNodeConn if the RtsqlMultiSink is already live locally.
   */
  private Set<String> mLocalMultiSinks;

  public EmbeddedFlumeConfig(Configuration conf) {
    mConf = conf;
    mForeignNodeConnections = new HashMap<String, ForeignNodeConn>();
    mLocalMultiSinks = new HashSet<String>();
    mFlumeNodes = new LinkedList<FlumeNode>();
    getHostName(); // resolve the hostname and cache the result.
    mIsRunning = false;
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
    if (mIsRunning) {
      LOG.warn("Superfluous call to start(): already running.");
      return;
    }

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
    mIsRunning = true;
  }

  /** @return true if the local Flume environment has been started. */
  public boolean isRunning() {
    return mIsRunning;
  }

  /** Submit a command to the Flume master and wait for it to finish executing. */
  private void submitCommand(FlumeMasterCommandThrift cmd) throws TException {
    mMasterClient.submit(cmd);
  }

  /**
   * Starts a Flume PhysicalNode that manages our data flows.
   */
  private void startPhysicalNode() {
    boolean startHttp = false;
    boolean isOneShot = false;

    mPhysicalNodeName = "rtsql-" + mHostName;
    FlumeNode node = new FlumeNode(mPhysicalNodeName, mFlumeConf, startHttp, isOneShot);
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

    // Configure the logical node to use our rtsqlsink and the user's source.
    spawnLogicalNode(nodeName, sourceStr, sinkStr);
  }

  /**
   * Removes the logical node for the specified flowSourceId from Flume.
   */
  public void stopFlowSink(String flowSourceId) throws TException {
    decommissionLogicalNode(flowSourceId);
  }

  /**
   * Spawn a new logical node, hosted on our physical node, which has the
   * specified source and sink.
   */
  public void spawnLogicalNode(String logicalNode, String source, String sink)
      throws TException {

    LOG.info("Spawning local logical node: " + logicalNode + ": " + source + " -> " + sink);

    // Put the node configuration in the master first.
    configureLogicalNode(logicalNode, source, sink);

    // Spawn the logical node on our local physical node.
    List<String> args = new ArrayList<String>();
    args.add(mPhysicalNodeName);
    args.add(logicalNode);
    FlumeMasterCommandThrift cmd = new FlumeMasterCommandThrift("spawn", args);
    submitCommand(cmd);
  }

  /**
   * Tell the Flume master to configure a given logical node with the specified
   * source and sink.
   */
  public void configureLogicalNode(String logicalNode, String source, String sink)
      throws TException {
    List<String> args = new ArrayList<String>();
    args.add(logicalNode);
    args.add(source);
    args.add(sink);
    LOG.info("Configuring logical node: " + logicalNode + " : " + source + " -> " + sink);
    FlumeMasterCommandThrift cmd = new FlumeMasterCommandThrift("config", args);
    submitCommand(cmd);
  }

  public void decommissionLogicalNode(String logicalNode) throws TException {
    List<String> args = new ArrayList<String>();
    args.add(logicalNode);
    FlumeMasterCommandThrift cmd = new FlumeMasterCommandThrift("decommission", args);
    submitCommand(cmd);
  }

  /**
   * Add a logical node name to the set of logical nodes we know are local
   * and have an RtsqlMultiSink in their output sink(s).
   */
  public void addLocalMultiSink(String nodeName) {
    mLocalMultiSinks.add(nodeName);
  }

  /**
   * Remove a logical node name from the set of nodes we know are local w/
   * RtsqlMultiSink output. This does not decommission the node itself.
   */
  public void dropLocalMultiSink(String nodeName) {
    mLocalMultiSinks.remove(nodeName);
  }

  /**
   * Starts an embedded instance of the FlumeMaster service.  Used when
   * flumebase is running in a "standalone" mode.
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
   * @return the source and sink specification for the specified logical node.
   *
   * Contacts the master and asks for the source/sink specification for a
   * logical node. Returns null if there is no such configuration, or else
   * a pair containing these two values.
   */
  public Pair<String, String> getNodeConfig(String logicalNodeName) throws TException {
    Map<String, ThriftFlumeConfigData> configMap = mMasterClient.getConfigs();

    ThriftFlumeConfigData nodeData = configMap.get(logicalNodeName);
    if (null == nodeData) {
      return null; // No such node.
    }

    return new Pair<String, String>(nodeData.getSourceConfig(), nodeData.getSinkConfig());
  }

  /**
   * Stop our Flume nodes.
   */
  public void stop() {
    LOG.info("Disconnecting from foreign resources");
    for (Map.Entry<String, ForeignNodeConn>  entry : mForeignNodeConnections.entrySet()) {
      String foreignName = entry.getKey();
      ForeignNodeConn foreignConn = entry.getValue();
      try {
        foreignConn.close();
      } catch (IOException ioe) {
        LOG.warn("Error disconnecting from foreign node " + foreignName + ": "
            + StringUtils.stringifyException(ioe));
      }
    }

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
    mIsRunning = false;
  }

  /**
   * Set up a connection from a foreign logical node 'nodeName' to forward a copy
   * of its data to our process.
   */
  private ForeignNodeConn connectToForeignNode(String nodeName) throws IOException {
    ForeignNodeConn newConn = null;
    ForeignNodeConn existingConn = null;
    synchronized (mForeignNodeConnections) {
      // Check if this is already a thing.
      existingConn = mForeignNodeConnections.get(nodeName);
      if (null == existingConn) {
        // We do not have an existing connection to this node.
        // Check to see if this node is actually the output of a CREATE STREAM AS SELECT.
        // If so, it's already hosting an RtsqlMultiSink; we don't need to create a receiver.
        if (mLocalMultiSinks.contains(nodeName)) {
          // Don't create a receiver, connect directly to the node.
          newConn = new LocalNodeConn(nodeName, mConf, this, nodeName);
        } else {
          // Create a new remote connection.
          newConn = new ForeignNodeConn(nodeName, mConf, this);
        }

        mForeignNodeConnections.put(nodeName, newConn);
      }
    }

    if (null != newConn) {
      // Actually open the connection here.
      newConn.connect();
      return newConn;
    }

    return existingConn;
  }

  /**
   * Connect to a foreign logical node 'nodeName' and open a local tap to deliver
   * events into the flow with the specified flowSourceId.
   *
   * <p>This actually opens a RtsqlMultiSink which fans out; we then add a local
   * RtsqlSink instance inside to receive events from it.</p>
   */
  public void addFlowToForeignNode(String nodeName, String flowSourceId) throws IOException {
    ForeignNodeConn conn = connectToForeignNode(nodeName);
    conn.addLocalSink(flowSourceId);
  }

  /**
   * Disconnect a local sink identified by flowSourceId from the local endpoint
   * for a connection to foreign node nodeName.
   */
  public void cancelForeignConn(String nodeName, String flowSourceId) throws IOException {
    LOG.info("Removing connection for flow " + flowSourceId
        + " from endpoint for foreign node " + nodeName);

    ForeignNodeConn conn = null;
    synchronized (mForeignNodeConnections) {
      conn = mForeignNodeConnections.get(nodeName);
    }

    if (null == conn) {
      throw new IOException("No such connection: " + nodeName);
    }

    conn.removeLocalSink(flowSourceId);
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
  public String getHostName() {
    if (null != mHostName) {
      return mHostName;
    }

    mHostName = NetUtils.getHostName();

    return mHostName;
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
