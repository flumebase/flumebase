// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.parser.StreamSourceType;

import com.odiago.rtengine.plan.CreateStreamNode;

/**
 * A Symbol representing a named stream.
 */
public class StreamSymbol extends Symbol {

  /** filename, logical node name, etc. */
  private final String mSource;

  /** True if the backing resource for this stream is local. */ 
  private final boolean mIsLocal;

  /** Initialize all parameters of a stream symbol explicitly. */
  public StreamSymbol(String name, StreamSourceType type, String source, boolean isLocal) {
    super(name, SymbolType.STREAM);
    mSource = source;
    mIsLocal = isLocal;
  }

  /** Initialize a stream symbol from the logical plan node for a CREATE STREAM operation. */
  public StreamSymbol(CreateStreamNode createNode) {
    super(createNode.getName(), SymbolType.STREAM);
    mSource = createNode.getSource();
    mIsLocal = createNode.isLocal();
  }

  public String getSource() {
    return mSource;
  }

  public boolean isLocal() {
    return mIsLocal;
  }
}
