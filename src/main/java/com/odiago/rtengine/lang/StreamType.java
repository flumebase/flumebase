// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.ArrayList;
import java.util.List;

/**
 * Type that defines a stream.
 */
public class StreamType extends Type {
  /** The types associated with each column. */
  private List<Type> mColumnTypes;

  public StreamType(List<Type> columnTypes) {
    super(TypeName.STREAM);
    mColumnTypes = new ArrayList<Type>();
    mColumnTypes.addAll(columnTypes);
  }

  private static final StreamType EMPTY_STREAM_TYPE = new StreamType(new ArrayList<Type>());

  /** @return the StreamType with no columns. */
  public static StreamType getEmptyStreamType() {
    return EMPTY_STREAM_TYPE;
  }

  @Override
  public boolean isPrimitive() {
    // Streams are recursive types.
    return false;
  }

  /**
   * @return an ordered list representation of the types of all the columns
   * of this stream. Callers should not modify the underlying list.
   */
  public List<Type> getColumnTypes() {
    return mColumnTypes;
  }

  @Override
  public int hashCode() {
    int ret = 0;
    for (Type t : mColumnTypes) {
      ret ^= t.hashCode();
    }

    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("STREAM(");
    boolean isFirst = true;
    for (Type t : mColumnTypes) {
      if (!isFirst) {
        sb.append(", ");
      }
      sb.append(t.toString());
      isFirst = false;
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other == null) {
      return false;
    } else if (!other.getClass().equals(getClass())) {
      return false;
    }

    StreamType otherType = (StreamType) other;
    List<Type> otherColumns = otherType.getColumnTypes();
    if (otherColumns.size() != mColumnTypes.size()) {
      return false; // Wrong number of columns.
    }

    for (int i = 0; i < mColumnTypes.size(); i++) {
      if (!mColumnTypes.get(i).equals(otherColumns.get(i))) {
        return false; // A column disagrees.
      }
    }

    // They seem equal.
    return true;
  }
  
}
