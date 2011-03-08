// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.client;

import java.io.IOException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.DummyExecEnv;
import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.FlowInfo;
import com.odiago.rtengine.exec.QuerySubmitResponse;

import com.odiago.rtengine.exec.local.LocalEnvironment;

import com.odiago.rtengine.server.ServerMain;
import com.odiago.rtengine.server.SessionId;
import com.odiago.rtengine.util.AppUtils;
import com.odiago.rtengine.util.QuitException;
import com.odiago.rtengine.util.StringUtils;

import jline.ConsoleReader;

/**
 * Client frontend to rtengine system.
 */
public class CmdLineClient {
  private static final String VERSION_STRING = "rtengine 1.0.0 / rtsql for Flume";

  private static final Logger LOG = LoggerFactory.getLogger(
      CmdLineClient.class.getName());

  /** Config key specifying what environment to autoconnect to. */
  public static final String AUTOCONNECT_URL_KEY = "rtengine.autoconnect";

  /** Default autoconnect environment is local. */
  public static final String DEFAULT_AUTOCONNECT_URL = "local";

  /** All config keys we pass to the environment start with this prefix. */
  public static final String RTENGINE_KEY_PREFIX = "rtengine.";

  /** Application configuration. */
  private Configuration mConf;

  /** True if we're mid-buffer on a command. */
  private boolean mInCommand;

  /** Buffer containing command being typed in over multiple lines. */
  private StringBuilder mCmdBuilder;

  /** The connected execution environment. */
  private ExecEnvironment mExecEnv;

  /** SessionId in the connected exec env. */
  private SessionId mSessionId;

  public CmdLineClient() {
    this(new Configuration());
  }

  public CmdLineClient(Configuration conf) {
    mConf = conf;
    resetCmdState();
  }

  /**
   * Print the version string to stdout.
   */
  private void printVersion() {
    System.out.println(VERSION_STRING);
  }

  /**
   * Format the currently-running flow info to stdout.
   */
  private void showFlows() {
    try {
      Map<FlowId, FlowInfo> flows = mExecEnv.listFlows();
      List<FlowId> watchList = mExecEnv.listWatchedFlows(mSessionId);
      System.out.println("Watch?\t" + FlowInfo.getHeader());
      for (Map.Entry<FlowId, FlowInfo> entry : flows.entrySet()) {
        if (watchList.contains(entry.getKey())) {
          System.out.print("  *\t"); // Put an asterisk in the front to signify watching.
        } else {
          System.out.print("   \t");
        }
        System.out.println(entry.getValue().toString());
      }
    } catch (Exception e) {
      LOG.error("Exception listing flows: " + StringUtils.stringifyException(e));
      return;
    }
  }

  /**
   * @return the set of configuration options to pass to the execEnvironment
   * governing a submitted query's behavior. This is all the elements in our
   * mConf that match the rtengine key prefix.
   */
  private Map<String, String> getQuerySettings() {
    Map<String, String> settings = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : mConf) {
      if (entry.getKey().startsWith(RTENGINE_KEY_PREFIX)) {
        settings.put(entry.getKey(), entry.getValue());
      }
    }

    // Set some values from our environment.
    settings.put(LocalEnvironment.SUBMITTER_SESSION_ID_KEY, Long.toString(mSessionId.getId()));

    return settings;
  }

  private void printUsage() {
    System.out.println("");
    System.out.println("All text commands must end with a ';' character.");
    System.out.println("Session control commands must be on a line by themselves.");
    System.out.println("");
    System.out.println("Session control commands:");
    System.out.println("  \\c                    Cancel the current input statement.");
    System.out.println("  \\d flowId             Drop the specified flow.");
    System.out.println("  \\D flowId             Drop a flow and wait for it to stop.");
    System.out.println("  \\dname flowId         Drop the stream name associated with flowId.");
    System.out.println("  \\disconnect           Disconnects from the server.");
    System.out.println("  \\f                    List flows.");
    System.out.println("  \\h                    Print help message.");
    System.out.println("  \\name flowId [str]    Set 'str' as the output stream name for flowId.");
    System.out.println("  \\open server [port]   Connects to the specified server.");
    System.out.println("  \\set property[=val]   Sets or retrieves configuration properties.");
    System.out.println("  \\shutdown!            Shuts down the server.");
    System.out.println("  \\q                    Quit the client.");
    System.out.println("  \\u, \\unwatch flowId   Stop watching a flow.");
    System.out.println("  \\w, \\watch flowId     Watch the output of a flow.");
    System.out.println("");
  }

  /**
   * Reset the current command buffer.
   */
  private void resetCmdState() {
    mInCommand = false;
    mCmdBuilder = new StringBuilder();
  }

  /**
   * Request that the execution environment stop the flow with the specified flowIdStr.
   */
  private void tryCancelFlow(String flowIdStr, boolean wait) {
    try {
      long idNum = Long.valueOf(flowIdStr);
      FlowId id = new FlowId(idNum);
      mExecEnv.cancelFlow(id);

      if (wait) {
        mExecEnv.joinFlow(id);
      }
    } catch (Exception e) {
      LOG.error("Exception while canceling flow: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Check if 'array' has at least 'requiredLen' elements. If not, print an error message.
   * @return true if array.length &gt;= requiredLen.
   */
  private boolean requireArgs(String [] array, int requiredLen) {
    if (array.length < requiredLen) {
      int printLen = requiredLen - 1; // array[0] is the command, not an "argument."
      System.err.println("Error: this command requires " + printLen + " argument(s).");
      return false;
    }

    return true;
  }

  /** Disconnect from the current ExecutionEnvironment. */
  private void disconnect() {
    if (null == mExecEnv) {
      return; // Nothing to do.
    }

    try {
      boolean isConnected = mExecEnv.isConnected();
      mExecEnv.disconnect(mSessionId); // Try anyway
      if (isConnected) {
        // Only display this message if we know for sure we were connected previously.
        LOG.info("Disconnected from execution environment.");
      }
      // Install a dummy environment until the user connects to a new one.
      mExecEnv = new DummyExecEnv();
    } catch (Exception e) {
      LOG.error("Error during disconnect: " + e);
    }
  }

  /** Shut down the connected execution environment. */
  private void shutdown() {
    try {
      mExecEnv.shutdown();
      LOG.info("Execution environment shut down.");
      mExecEnv = new DummyExecEnv();
    } catch (Exception e) {
      LOG.error("Error during shutdown: " + e);
    }
  }

  /**
   * Connect to a new execution environment.
   * @param host the host to connect to. If this is 'local', use the LocalEnvironment.
   * Otherwise, assume this is of the form 'hostname[:port]' and parse accordingly.
   */
  private void connect(String host) {
    disconnect(); // Always disconnect from the current environment before reconnecting. 
    try {
      if ("local".equals(host)) {
        LOG.info("Connecting to local environment.");
        mExecEnv = new LocalEnvironment(mConf);
      } else if ("none".equals(host)) {
        mExecEnv = new DummyExecEnv();
      } else {
        int portIndex = host.indexOf(':');
        int port = mConf.getInt(ServerMain.THRIFT_SERVER_PORT_KEY,
            ServerMain.DEFAULT_THRIFT_SERVER_PORT);
        String hostname = host;
        if (portIndex != -1) {
          port = Integer.valueOf(host.substring(portIndex + 1));
          hostname = host.substring(0, portIndex);
        }

        LOG.info("Connecting to remote environment at " + hostname + " on port " + port);
        mExecEnv = new ThriftClientEnvironment(mConf, hostname, port);
      }

      mSessionId = mExecEnv.connect();
    } catch (Exception e) {
      LOG.error("Could not connect to environment: " + e);
    }
  }

  /** print all properties that start with 'prefix'. */
  private void printProperties(String prefix) {
    for (Map.Entry<String, String> entry : mConf) {
      if (null == prefix || entry.getKey().startsWith(prefix)) {
        System.out.println(entry.getKey() + " = '" + entry.getValue() + "'");
      }
    }
  }

  /**
   * Prints or sets the specified property.
   * If propKeyVal is of the form 'k=v', sets conf[k] = v. Otherwise, if it
   * is just a property key, prints the value of that property.
   */
  private void setProperty(String propKeyVal) {
    int equalIdx = propKeyVal.indexOf('=');
    String key = null;
    if (-1 != equalIdx) {
      key = propKeyVal.substring(0, equalIdx);
      String value = propKeyVal.substring(equalIdx + 1);
      mConf.set(key, value);
      System.out.println(key + " = '" + value + "'");
    } else {
      // Just a key.
      key = propKeyVal;

      if (key.endsWith(".")) {
        // Actually it's a prefix. Print out everything that starts with this.
        printProperties(key);
      } else {
        // Print out the value for this key.
        String outVal = mConf.get(key);
        if (null != outVal) {
          System.out.println(key + " = '" + outVal + "'");
        } else {
          System.out.println("No such property: " + key);
        }
      }
    }

  }

  /**
   * Watch the output of the specified flow.
   */
  private void watch(String flowIdStr) {
    try {
      long idNum = Long.valueOf(flowIdStr);
      FlowId flowId = new FlowId(idNum);
      mExecEnv.watchFlow(mSessionId, flowId);

    } catch (Exception e) {
      LOG.error("Exception subscribing to flow: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Stop watching the output of the specified flow.
   */
  private void unwatch(String flowIdStr) {
    try {
      long idNum = Long.valueOf(flowIdStr);
      FlowId flowId = new FlowId(idNum);
      mExecEnv.unwatchFlow(mSessionId, flowId);

    } catch (Exception e) {
      LOG.error("Exception unsubscribing from flow: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Set the stream name associated with a flow Id.
   */
  private void setFlowName(String flowIdStr, String streamName) {
    try {
      long idNum = Long.valueOf(flowIdStr);
      FlowId flowId = new FlowId(idNum);
      mExecEnv.setFlowName(flowId, streamName);
      if (null == streamName) {
        System.out.println("Removed stream name from flow " + flowIdStr);
      } else {
        System.out.println("Created stream '" + streamName + "' on flow " + flowIdStr);
      }
    } catch (Exception e) {
      LOG.error("Exception setting flow name: " + StringUtils.stringifyException(e));
    }
  }

  private void getFlowName(String flowIdStr) {
    try {
      long idNum = Long.valueOf(flowIdStr);
      FlowId flowId = new FlowId(idNum);

      Map<FlowId, FlowInfo> infoMap = mExecEnv.listFlows();
      FlowInfo info = infoMap.get(flowId);
      if (null == info) {
        System.out.println("No such flow: " + flowIdStr);
      } else if (null != info.streamName) {
        System.out.println(info.streamName);
      } else {
        System.out.println("(none)");
      }
    } catch (Exception e) {
      LOG.error("Exception retrieving flow name: " + StringUtils.stringifyException(e));
    }
  }

  /**
   * Handle a '\x' event for various values of the escape character 'x'.
   * @param escapeChar the first character following the '\\'.
   * @param args All whitespace-delimited substrings of the command; args[0] is "\\x".
   */
  private void handleEscape(char escapeChar, String[] args) throws QuitException {
    // Start by handling the multi-character escapes.
    if (args[0].equals("\\disconnect")) {
      disconnect();
    } else if (args[0].equals("\\shutdown!")) {
      shutdown();
    } else if (args[0].equals("\\open")) {
      if (requireArgs(args, 2)) {
        connect(args[1]);
      }
    } else if (args[0].equals("\\set")) {
      if (args.length == 1) {
        printProperties(null);
      } else if (args.length == 2) {
        setProperty(args[1]);
      } else {
        System.err.println("Unknown syntax.");
      }
    } else if (args[0].equals("\\watch") || args[0].equals("\\w")) {
      if (requireArgs(args, 2)) {
        watch(args[1]);
      }
    } else if (args[0].equals("\\unwatch") || args[0].equals("\\u")) {
      if (requireArgs(args, 2)) {
        unwatch(args[1]);
      }
    } else if (args[0].equals("\\name")) {
      if (args.length >= 3) {
        setFlowName(args[1], args[2]);
      } else if (requireArgs(args, 2)) {
        getFlowName(args[1]);
      }
    } else if (args[0].equals("\\dname")) {
      if (requireArgs(args, 2)) {
        setFlowName(args[1], null);
      }
    } else if (args[0].length() == 2) {
      // Handle the one-character escapes here:
      switch(escapeChar) {
      case 'c':
        resetCmdState();
        break;
      case 'd':
        if (requireArgs(args, 2)) {
          tryCancelFlow(args[1], false);
        }
        break;
      case 'D':
        if (requireArgs(args, 2)) {
          tryCancelFlow(args[1], true);
        }
        break;
      case 'f':
        showFlows();
        break;
      case 'h':
        printUsage();
        break;
      case 'q':
        // Graceful quit from the shell.
        throw new QuitException(0);
      default:
        System.err.println("Unknown control command: " + args[0]);
        System.err.println("Try \\h for help.");
        break;
      }
    } else {
      System.err.println("Unknown control command: " + args[0]);
    }
  }

  /**
   * Given a command which may contain leading or trailing whitespace
   * and a final ';' at the end, remove these unnecessary characters.
   */
  private String trimTerminator(String cmdIn) {
    String trimmed = cmdIn.trim();
    if (trimmed.endsWith(";")) {
      // Remove the trailing ';' character.
      trimmed = trimmed.substring(0, trimmed.length() - 1);
      // After trimming the final ';', there may be additional whitespace to trim.
      trimmed = trimmed.trim();
    }

    return trimmed;
  }
  /**
   * Parse and execute a text command.
   */
  private void execCommand(String cmd) throws InterruptedException, IOException, QuitException {
    String realCommand = trimTerminator(cmd);
    
    if (realCommand.equalsIgnoreCase("help")) {
      printUsage();
    } else if (realCommand.equalsIgnoreCase("exit")) {
      throw new QuitException(0);
    } else {
      QuerySubmitResponse response = mExecEnv.submitQuery(realCommand, getQuerySettings());
      String msg = response.getMessage();
      if (null != msg) {
        System.out.println(msg);
      }

      FlowId flowId = response.getFlowId();
      if (null != flowId) {
        System.out.println("Started flow: " + flowId);
      }
    }
  }

  private void printPrompt() {
    if (mInCommand) {
      System.out.print("    -> ");
    } else {
      System.out.print("rtsql> ");
    }
  }

  /**
   * Main entry point for the command-line client.
   * @return the exit code for the program (0 on success).
   */
  public int run() throws IOException {
    System.out.println("Welcome to the rtengine client.");
    printVersion();
    System.out.println("Type 'help;' or '\\h' for instructions.");
    System.out.println("Type 'exit;' or '\\q' to quit.");

    connect(mConf.get(AUTOCONNECT_URL_KEY, DEFAULT_AUTOCONNECT_URL));

    HistoryFile history = new HistoryFile();
    ConsoleReader conReader = new ConsoleReader();
    history.populateConsoleReader(conReader);
    history.open();

    mInCommand = false;
    mCmdBuilder = new StringBuilder();

    try {
      while (true) {
        printPrompt();
        String line = conReader.readLine();
        if (null == line) {
          // EOF on input. We're done.
          throw new QuitException(0);
        } else if (line.endsWith("\\c")) {
          // Lines ending in '\\c' cancel the current input.
          resetCmdState();
          continue;
        }
        String trimmed = line.trim();
        if (trimmed.startsWith("\\")) {
          if (trimmed.length() == 1) {
            System.err.println("Control sequence '\\' requires a command character. Try \\h");
          } else {
            String[] args = trimmed.split("[ \\t]+");
            handleEscape(trimmed.charAt(1), args);
          }
          resetCmdState();
        } else if (trimmed.endsWith(";")) {
          mCmdBuilder.append(line);
          mCmdBuilder.append('\n');
          try {
            execCommand(mCmdBuilder.toString());
          } catch (InterruptedException ie) {
            LOG.warn("Interrupted while processing command: " + ie);
          }
          history.logCommand(mCmdBuilder.toString());
          resetCmdState();
        } else if (line.length() > 0) {
          mCmdBuilder.append(line);
          mCmdBuilder.append('\n');
          mInCommand = true;
        }
      }
    } catch (QuitException qe) {
      System.out.println("Goodbye");
      return qe.getStatus();
    } finally {
      try {
        history.close();
      } catch (IOException ioe) {
        LOG.warn("IOException closing history file: " + ioe);
      }

      try {
        mExecEnv.disconnect(mSessionId);
      } catch (InterruptedException ie) {
        LOG.warn("Interruption while disconnecting from service: " + ie);
      }
    }
  }

  public static void main(String [] args) throws Exception {
    AppUtils.initLogging();
    Configuration conf = AppUtils.initConfResources();
    CmdLineClient client = new CmdLineClient(conf);
    System.exit(client.run());
  }
}
