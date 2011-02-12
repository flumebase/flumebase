// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.testutil;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.exec.StreamSymbol;

import com.odiago.rtengine.lang.StreamType;
import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.FormatSpec;
import com.odiago.rtengine.parser.StreamSourceType;
import com.odiago.rtengine.parser.TypedField;

/**
 * Builder that creates a StreamSymbol.
 */
public class StreamBuilder {
  private List<TypedField> mFields;
  private String mStreamName;
  private FormatSpec mFormatSpec;
  private StreamSourceType mSourceType;
  private boolean mIsLocal;
  private String mSource;

  public StreamBuilder() {
    this(null);
  }

  public StreamBuilder(String name) {
    mStreamName = name;
    mFields = new ArrayList<TypedField>();
    mFormatSpec = new FormatSpec();
    mSourceType = StreamSourceType.Memory;
    mSource = null;
    mIsLocal = false;
  }

  public void setName(String name) {
    mStreamName = name;
  }

  public String getName() {
    return mStreamName;
  }

  public void setFormat(FormatSpec fmt) {
    mFormatSpec = fmt;
  }

  public FormatSpec getFormat() {
    return mFormatSpec;
  }

  public void setSourceType(StreamSourceType srcType) {
    mSourceType = srcType;
  }

  public StreamSourceType getSourceType() {
    return mSourceType;
  }
  
  public void setSource(String src) {
    mSource = src;
  }

  public String getSource() {
    return mSource;
  }

  public void setLocal(boolean isLocal) {
    mIsLocal = isLocal;
  }

  public boolean getLocal() {
    return mIsLocal;
  }

  public void addField(TypedField tf) {
    mFields.add(tf);
  }

  public void addField(String fieldName, Type fieldType) {
    addField(new TypedField(fieldName, fieldType));
  }

  public List<TypedField> getFields() {
    return mFields;
  }

  /**
   * @return a StreamType instance specifying the type of this stream.
   */
  protected StreamType makeStreamType() {
    List<Type> colTypes = new ArrayList<Type>();
    for (TypedField field : mFields) {
      colTypes.add(field.getType());
    }

    return new StreamType(colTypes);
  }

  /**
   * @return the InMemStreamSymbol representing this stream.
   */
  public StreamSymbol build() {
    if (null == mStreamName) {
      throw new RuntimeException("Must call setName() to name the stream before building");
    }

    if (null == mSource) {
      throw new RuntimeException("Must call setSource() first. "
          + "You probably want setSourceType() too.");
    }

    if (0 == mFields.size()) {
      throw new RuntimeException("Must define at least one field with addField()");
    }

    return new StreamSymbol(mStreamName, mSourceType, makeStreamType(),
        mSource, mIsLocal, new ArrayList<TypedField>(mFields), mFormatSpec);
  }
}
