// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.lang.StreamType;

import com.odiago.rtengine.parser.StreamSourceType;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.plan.CreateStreamNode;

/**
 * A Symbol representing a named stream.
 */
public class StreamSymbol extends Symbol {

  /** filename, logical node name, etc. */
  private final String mSource;

  /** True if the backing resource for this stream is local. */ 
  private final boolean mIsLocal;

  /** The source of the stream (a file, a Flume EventSource, etc.) */
  private final StreamSourceType mStreamType;

  private final List<TypedField> mFieldTypes;

  /** Initialize all parameters of a stream symbol explicitly. */
  public StreamSymbol(String name, StreamSourceType type, String source, boolean isLocal) {
    super(name, StreamType.getEmptyStreamType());
    mSource = source;
    mStreamType = type;
    mIsLocal = isLocal;
    mFieldTypes = new ArrayList<TypedField>();
  }

  /** Initialize a stream symbol from the logical plan node for a CREATE STREAM operation. */
  public StreamSymbol(CreateStreamNode createNode) {
    super(createNode.getName(), new StreamType(createNode.getFieldsAsTypes()));
    mSource = createNode.getSource();
    mIsLocal = createNode.isLocal();
    mStreamType = createNode.getType();
    mFieldTypes = new ArrayList<TypedField>(createNode.getFields());
  }

  /**
   * @return a set of TypedFields declaring the names and types of all fields.
   * The objects in this list should not be modified by the client.
   */
  public List<TypedField> getFields() {
    return mFieldTypes;
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
      sb.append("  local\n");
    }
    sb.append("  fields:\n");
    for (TypedField field : mFieldTypes) {
      sb.append("    ");
      sb.append(field);
      sb.append("\n");
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    boolean parentEquals = super.equals(other);
    if (!parentEquals) {
      return true;
    }

    StreamSymbol otherStream = (StreamSymbol) other;
    return mIsLocal == otherStream.mIsLocal
        && mSource.equals(otherStream.mSource)
        && mStreamType.equals(otherStream.mStreamType);
  }

}
