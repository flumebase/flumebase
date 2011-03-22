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

package com.odiago.flumebase.lang;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.util.StringUtils;

/**
 * Type that defines a stream.
 */
public class StreamType extends Type {
  private static final Logger LOG = LoggerFactory.getLogger(StreamType.class.getName());

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
  public Schema getAvroSchema() {
    // TODO(aaron): Would be good to create avro schemas for the whole stream, for
    // completeness' sake (not currently required here).
    // Note that since we do not have the field names, we cannot create a proper
    // record schema in here. For an example of creating a schema for a set of fields,
    // see SelectStmt.createFieldSchema().
    LOG.error("StreamType.getAvroSchema() -- not implemented!");
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("STREAM(");
    StringUtils.formatList(sb, mColumnTypes);
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
