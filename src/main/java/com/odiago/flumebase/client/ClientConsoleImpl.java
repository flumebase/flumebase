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
