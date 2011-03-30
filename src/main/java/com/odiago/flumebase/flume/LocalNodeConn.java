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

import org.apache.hadoop.conf.Configuration;

/**
 * Provide an interface compatible with ForeignNodeConn, but don't actually
 * manage the creation/destruction of a local -receiver node. The RtsqlMultiSink
 * we need is already available from a different locally-hosted node, so we
 * directly interface with that.
 */
public class LocalNodeConn extends ForeignNodeConn {

  /** id of the RtsqlMultiSink associated with this local node. */
  private final String mLocalSinkName;

  public LocalNodeConn(String foreignNodeName, Configuration conf,
      EmbeddedFlumeConfig flumeConf, String localSinkName) {
    super(foreignNodeName, conf, flumeConf);
    mLocalSinkName = localSinkName;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isConnected() {
    // The local node already exists, so this "connection" is always present.
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public void connect() {
    // Nothing to do. Suppress parent implementation behavior.
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    // Nothing to do. Suppress parent implementation behavior.
  }

  /** {@inheritDoc} */
  @Override
  protected String getMultiSinkId() {
    return mLocalSinkName;
  }
}
