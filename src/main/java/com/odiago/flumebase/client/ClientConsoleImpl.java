// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.client;

import com.odiago.flumebase.thrift.ClientConsole;

/**
 * Implementation of the ClientConsole rpc server.
 * This is actually hosted on the client; it is a receiver of info
 * to print for the client. The server connects back to this RPC
 * after the client first connects to the server.
 */
public class ClientConsoleImpl implements ClientConsole.Iface {
  /** Config key specifying the port where the console is hosted. */
  public static final String CONSOLE_SERVER_PORT_KEY = "flumebase.console.port";

  public static final int DEFAULT_CONSOLE_SERVER_PORT = 9293;

  /** Print ordinary info / records from the server to the console. */
  @Override
  public void sendInfo(String info) {
    System.out.println(info);
  }

  /** Print error information to the console. */
  @Override
  public void sendErr(String err) {
    System.err.println(err);
  }

}
