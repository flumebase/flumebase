// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util;

import com.odiago.rtengine.client.CmdLineClient;

/** 
 * Retrieves version information from the package jar for this version of rtengine.
 */
public final class VersionInfo {

  private VersionInfo() {
  }

  /** 
   * @return the version string for rtengine.
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
    System.out.println("rtengine version " + getVersionString());
  }
}
