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

package com.odiago.flumebase.server;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TProcessor;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.thrift.RemoteServer;

import com.odiago.flumebase.util.AppUtils;

/**
 * Main entry-point class for the remote execution server.
 */
public class ServerMain {
  private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class.getName());

  public static final String THRIFT_SERVER_PORT_KEY = "flumebase.remote.port";
  public static final int DEFAULT_THRIFT_SERVER_PORT = 9292;

  /** Configuration info for the server. */
  private Configuration mConf;

  public ServerMain() {
    this(new Configuration());
  }

  public ServerMain(Configuration conf) {
    mConf = conf;
  }

  public int run(String [] args) throws Exception {
    LOG.info("Server is starting");

    RemoteServerImpl remoteImpl = new RemoteServerImpl(mConf);

    int port = mConf.getInt(THRIFT_SERVER_PORT_KEY, DEFAULT_THRIFT_SERVER_PORT);
    TServerTransport transport = new TServerSocket(port);
    TTransportFactory transportFactory = new TFramedTransport.Factory();
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TProcessor processor = new RemoteServer.Processor(remoteImpl);

    TServer server = new TThreadPoolServer(processor, transport, transportFactory,
        protocolFactory);

    LOG.info("Starting processing thread");
    remoteImpl.setServer(server);
    remoteImpl.start();

    try {
      LOG.info("Serving on port " + port);
      server.serve();
    } finally {
      LOG.info("Shutting down processing thread");
      if (remoteImpl.isRunning()) {
        remoteImpl.shutdown();
      }
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    AppUtils.initLogging();
    Configuration conf = AppUtils.initConfResources();
    ServerMain server = new ServerMain(conf);
    System.exit(server.run(args));
  }
}
