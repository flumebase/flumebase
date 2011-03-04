// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.io.EventParser;

import com.odiago.rtengine.lang.StreamType;
import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.FormatSpec;
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

  /** Specifies how the fields are parsed. */
  private final FormatSpec mFormatSpec;

  /** Initialize all parameters of a stream symbol explicitly.
   * @param name the name of the stream.
   * @param sourceType specifies where the source data for this stream comes from
   * (e.g., flume, a file, etc..)
   * @param streamType the type signature for the stream.
   * @param source the specification for how to connect to the source.
   * @param isLocal true if the source of this data is local.
   * @param fieldTypes the names and types for each field.
   * @param fmt the FormatSpec that tells how to parse the stream events.
   */
  public StreamSymbol(String name, StreamSourceType sourceType, Type streamType,
      String source, boolean isLocal, List<TypedField> fieldTypes, FormatSpec fmt) {
    super(name, streamType);
    mSource = source;
    mStreamType = sourceType;
    mIsLocal = isLocal;
    mFieldTypes = new ArrayList<TypedField>();
    if (null != fieldTypes) {
      mFieldTypes.addAll(fieldTypes);
    }
    mFormatSpec = fmt;
  }

  /** Initialize a stream symbol from the logical plan node for a CREATE STREAM operation. */
  public StreamSymbol(CreateStreamNode createNode) {
    super(createNode.getName(), new StreamType(createNode.getFieldsAsTypes()));
    mSource = createNode.getSource();
    mIsLocal = createNode.isLocal();
    mStreamType = createNode.getType();
    mFieldTypes = new ArrayList<TypedField>(createNode.getFields());
    mFormatSpec = createNode.getFormatSpec();
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

  public FormatSpec getFormatSpec() {
    return mFormatSpec;
  }

  /**
   * @return an EventParser for events coming from this stream.
   */
  public EventParser getEventParser() {
    return mFormatSpec.getEventParser();
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
    sb.append("  format: ");
    sb.append(mFormatSpec.getFormat());
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
      return false;
    }

    StreamSymbol otherStream = (StreamSymbol) other;
    return mIsLocal == otherStream.mIsLocal
        && mSource.equals(otherStream.mSource)
        && mStreamType.equals(otherStream.mStreamType)
        && mFormatSpec.equals(otherStream.mFormatSpec);
  }

  @Override
  public Symbol withName(String name) {
    return new StreamSymbol(name, mStreamType,  getType(), mSource, mIsLocal,
        mFieldTypes, mFormatSpec);
  }
}
