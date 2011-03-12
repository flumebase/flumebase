// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util;

import com.odiago.flumebase.client.CmdLineClient;

/** 
 * Retrieves version information from the package jar for this version of FlumeBase.
 */
public final class VersionInfo {

  private VersionInfo() {
  }

  /** 
   * @return the version string for FlumeBase.
   */
  public static String getVersionString() {
    // CmdLineClient is specified as the "main class" for our jar.
    String ver = CmdLineClient.class.getPackage().getImplementationVersion();
    if (null == ver) {
      ver = "(unknown)";
    }
    return ver;
  }

  public static void main(String[] args) {
    System.out.println("FlumeBase version " + getVersionString());
  }
}
