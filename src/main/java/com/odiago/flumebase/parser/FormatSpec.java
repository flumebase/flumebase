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

package com.odiago.flumebase.parser;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.io.AvroEventParser;
import com.odiago.flumebase.io.DelimitedEventParser;
import com.odiago.flumebase.io.EventParser;

/**
 * Specifies how events in a given stream are parsed and what parameters
 * govern that process.
 */
public class FormatSpec extends SQLStatement {
  private static final Logger LOG = LoggerFactory.getLogger(FormatSpec.class.getName());

  /** The default format; uses a delimiter character. */
  public static final String FORMAT_DELIMITED = "delimited";

  /** Specifies that the regular delimited format should be used. */
  public static final String DEFAULT_FORMAT_NAME = FORMAT_DELIMITED;

  /** Binary Avro encoding format. Requires an input schema. */
  public static final String FORMAT_AVRO = "avro";

  /** The name of the event format, which dictates the EventParser implementation to use. */
  private String mFormat;

  /** Free-form parameter map for EventParser-specific configuration. */
  private Map<String, String> mParams;

  public FormatSpec() {
    this(DEFAULT_FORMAT_NAME);
  }

  public FormatSpec(String format) {
    mFormat = format;
    mParams = new HashMap<String, String>();
  }

  public String getFormat() {
    return mFormat;
  }

  public void setFormat(String fmt) {
    mFormat = fmt;
  }

  public Map<String, String> getParams() {
    return mParams;
  }

  public String getParam(String key) {
    return mParams.get(key);
  }

  public void setParam(String key, String val) {
    mParams.put(key, val);
  }

  /**
   * @return an EventParser as we configured it.
   */
  public EventParser getEventParser() {
    if (FORMAT_DELIMITED.equals(mFormat)) {
      return new DelimitedEventParser(mParams);
    } else if (FORMAT_AVRO.equals(mFormat)) {
      return new AvroEventParser(mParams);
    }

    LOG.error("No EventParser with format name: " + mFormat);
    return null;
  }


  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("FormatSpec mFormat=");
    sb.append(mFormat);
    sb.append(" mParams:\n");
    for (Map.Entry<String, String> param : mParams.entrySet()) {
      pad(sb, depth + 1);
      sb.append(param.getKey());
      sb.append(" = ");
      sb.append(param.getValue());
      sb.append("\n");
    }
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }
    FormatSpec other = (FormatSpec) otherObj;

    return mFormat.equals(other.mFormat) && mParams.equals(other.mParams);
  }
}
