// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class NetUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NetUtils.class.getName());

  private NetUtils() { }

  /**
   * @return the local hostname.
   */
  public static String getHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      LOG.warn("Could not determine local hostname: " + uhe);
      return "localhost";
    }
  }

}
