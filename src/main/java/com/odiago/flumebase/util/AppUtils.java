// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.log4j.PropertyConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods regarding application lifecycle management (startup,
 * logging init, etc.)
 */
public final class AppUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AppUtils.class.getName());

  /** System property that sets the path of the FlumeBase configuration. */
  private static final String FLUMEBASE_CONF_DIR_KEY = "flumebase.conf.dir";

  /** Environment variable that sets the path of the FlumeBase configuration. */
  private static final String FLUMEBASE_CONF_DIR_ENV = "FLUMEBASE_CONF_DIR";

  /** Environment variable that sets the path of the FlumeBase installation "home". */
  private static final String FLUMEBASE_HOME_ENV = "FLUMEBASE_HOME";

  private AppUtils() { }

  /**
   * Initialize slf4j / log4j for the application.
   */
  public static void initLogging() {
    String confDir = getAppConfDir();
    File confDirFile = new File(confDir);
    File log4jPropertyFile = new File(confDirFile, "log4j.properties");
    FileInputStream fis = null;
    try {
      // Load the properties from the file first.
      fis = new FileInputStream(log4jPropertyFile);
      Properties props = new Properties();
      props.load(fis);

      // Overlay the system properties on top of the file, in case the
      // user wants to override the file settings at invocation time.
      Properties sysProps = System.getProperties();
      props.putAll(sysProps);
      PropertyConfigurator.configure(props);
      LOG.debug("Logging enabled.");
    } catch (IOException ioe) {
      System.err.println("IOException encoutered while initializing logging.");
      System.err.println("The logging system might not be ready, meaning you");
      System.err.println("might miss many other messages. Do you have an");
      System.err.println("etc/log4j.properties file in this installation?");
      System.err.println("Specific exception follows:");
      System.err.println(ioe);
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException ioe) {
          LOG.warn("Exception closing log4j.properties file: " + ioe);
        }
      }
    }
  }

  /**
   * @return the home directory for this application installation.
   * This is taken from $FLUMEBASE_HOME. Returns null if this is not set.
   */
  public static String getAppHomeDir() {
    String homeEnv = System.getenv(FLUMEBASE_HOME_ENV);
    if (null != homeEnv) {
      File homeFile = new File(homeEnv);
      return homeFile.getAbsolutePath();
    } else {
      return null;
    }
  }

  /**
   * @return the configuration directory for this application.
   * This is one of the following, in order:
   * <ol>
   *   <li>${flumebase.conf.dir} (System property)</li>
   *   <li>$FLUMEBASE_CONF_DIR</li>
   *   <li>$FLUMEBASE_HOME/etc</li>
   *   <li>(current dir)</li>
   * </ol>
   */
  public static String getAppConfDir() {
    String flumebaseConfDir = System.getProperty(FLUMEBASE_CONF_DIR_KEY, null);
    if (null == flumebaseConfDir) {
      flumebaseConfDir = System.getenv(FLUMEBASE_CONF_DIR_ENV);
    }

    if (null != flumebaseConfDir) {
      File confFile = new File(flumebaseConfDir);
      return confFile.getAbsolutePath();
    }

    // Infer from application home dir. 
    String homeDir = getAppHomeDir();
    if (null != homeDir) {
      // return $FLUMEBASE_HOME/etc.
      File homeFile = new File(homeDir);
      return new File(homeFile, "etc").getAbsolutePath();
    }

    return ".";
  }

  /**
   * @return a Configuration with the appropriate flumebase-site.xml file added.
   * Should be used as the base Configuration for the program.
   */
  public static Configuration initConfResources() {
    String flumebaseConfFile = new File(getAppConfDir(), "flumebase-site.xml").toString();
    LOG.debug("Initializing configuration from " + flumebaseConfFile);
    Configuration conf = new Configuration();
    conf.addResource(new Path(flumebaseConfFile));
    return conf;
  }
}
