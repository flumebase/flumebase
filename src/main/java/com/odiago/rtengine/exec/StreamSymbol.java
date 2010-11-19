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

  private final StreamSourceType mStreamType;

  /** Initialize all parameters of a stream symbol explicitly. */
  public StreamSymbol(String name, StreamSourceType type, String source, boolean isLocal) {
    super(name, SymbolType.STREAM);
    mSource = source;
    mStreamType = type;
    mIsLocal = isLocal;
  }

  /** Initialize a stream symbol from the logical plan node for a CREATE STREAM operation. */
  public StreamSymbol(CreateStreamNode createNode) {
    super(createNode.getName(), SymbolType.STREAM);
    mSource = createNode.getSource();
    mIsLocal = createNode.isLocal();
    mStreamType = createNode.getType();
  }

  public String getSource() {
    return mSource;
  }

  public boolean isLocal() {
    return mIsLocal;
  }

  /**
   * @return the StreamSourceType identifying the origin of this stream.
   */
  public StreamSourceType getSourceType() {
    return mStreamType;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append("\n");
    sb.append("  type: ");
    sb.append(mStreamType);
    sb.append("\n");
    sb.append("  source: ");
    sb.append(mSource);
    sb.append("\n");
    if (mIsLocal) {
      sb.append("  local");
    }

    return sb.toString();
  }

}
