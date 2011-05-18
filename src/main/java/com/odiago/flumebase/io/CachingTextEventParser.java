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

package com.odiago.flumebase.io;

import java.nio.CharBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.util.Ref;

/**
 * Abstract base class for EventParser implementations.
 * This class makes it easier to write EventParsers that have the following
 * two properties:
 * <ul>
 *   <li>The input data is text, which will be tokenized in some fashion</li>
 *   <li>The text for each field should be cached, and if called upon by
 *   the user, converted to the appropriate data type.</li>
 * </ul>
 */
public abstract class CachingTextEventParser extends EventParser {

  private static final Logger LOG = LoggerFactory.getLogger(
      CachingTextEventParser.class.getName());

  /** key in the stream properties map specifying an escape to signify a null string. */
  public static final String NULL_STR_PARAM = "null.sequence";
  public static final String DEFAULT_NULL_STR = "\\N";

  /** key in the stream properties that specifies a split token for LIST<T> items. */
  public static final String LIST_SEPARATOR_PARAM = "list.delim";
  public static final String DEFAULT_LIST_SEPARATOR = "|";

  /** CharBuffers wrapping the text of each column. */
  private ArrayList<CharBuffer> mColTexts;

  /** The reified instances of the columns in their final types. A null
   * here may mean 'uncached', or true null, if mColumnNulls[i] is true. */
  private ArrayList<Object> mColumnValues;

  /** Array of t/f values; if true, indicates that we parsed the column
   * in question, but determined the value to be null. */
  private ArrayList<Boolean> mColumnNulls;
  
  /** An escape sequence that specifies that the current field is a null string. */
  private String mNullStr;

  /** Delimiter for list items. */
  private String mListSep;

  protected CachingTextEventParser() {
    mNullStr = DEFAULT_NULL_STR;
    mListSep = DEFAULT_LIST_SEPARATOR;
    init();
  }

  protected CachingTextEventParser(Map<String, String> params) {
    // Specify a char sequence that represents a null string.
    mNullStr = params.get(NULL_STR_PARAM);
    if (null == mNullStr) {
      mNullStr = DEFAULT_NULL_STR;
    }

    mListSep = params.get(LIST_SEPARATOR_PARAM);
    if (null == mListSep) {
      mListSep = DEFAULT_LIST_SEPARATOR;
    }

    init();
  }

  private void init() {
    mColTexts = new ArrayList<CharBuffer>();
    mColumnValues = new ArrayList<Object>();
    mColumnNulls = new ArrayList<Boolean>();
  }

  /**
   * Clear all internal state and reset to a new unparsed event body.
   * Overriding implementations must call super.reset(e).
   */
  @Override
  public void reset(Event e) {
    mColTexts.clear();
    mColumnValues.clear();
    mColumnNulls.clear();
  }

  public abstract Object getColumn(int colIdx, Type expectedType) throws ColumnParseException;

  /**
   * Given a CharBuffer wrapping a field, return the field's value in the
   * type expected by the runtime. Before returning, cache it in the slot for
   * 'colIdx'.
   */
  protected Object parseAndCache(CharBuffer chars, int colIdx, Type expectedType)
      throws ColumnParseException {

    String debugInputString = null;
    if (LOG.isDebugEnabled()) {
      // Save this for the end. This method may consume the chars object,
      // so we need to do this up-front.
      debugInputString = chars.toString();
    }

    Object out = CharBufferUtils.parseType(chars, expectedType, mNullStr, mListSep);

    while(mColumnValues.size() < colIdx) {
      // Add nulls to the list to increase the memoized size up to this column.
      mColumnValues.add(null);

      // Add a 'false' in the mColumnNulls so we know these are "padding" nulls
      // and not "true" nulls.
      mColumnNulls.add(Boolean.valueOf(false));
    }

    // Now add this parsed value to the end of the list. Sets its null bit appropriately.
    mColumnValues.add(out);
    mColumnNulls.add(Boolean.valueOf(out == null));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsed string [" + debugInputString + "] with expected type ["
          + expectedType + "] for column idx=" + colIdx + "; result is [" + out + "]"); 
    }
    return out;
  }

  /**
   * @return the list of cached values for columns.
   */
  protected List<Object> getColumnValues() {
    return Collections.unmodifiableList(mColumnValues);
  }

  /**
   * @return the list of cached is-null/is-not-null values for columns.
   */
  protected List<Boolean> getColumnNulls() {
    return Collections.unmodifiableList(mColumnNulls);
  }

  /**
   * @return the list of cached unparsed string representations of columns.
   */
  protected List<CharBuffer> getColumnTexts() {
    return Collections.unmodifiableList(mColTexts);
  }

  /**
   * Cache the unparsed text representation of the next column in the event.
   */
  protected void cacheColText(CharBuffer colText) {
    mColTexts.add(colText);
  }

  /**
   * Check whether the cache already contains a value for this column index.
   * If so, put the value in the 'out' parameter. If we've cached the column
   * text but not parsed it, we parse it here, cache the result, and then
   * stick that in the 'out' parameter.
   *
   * @param colIdx the index of the column into the list
   * @param expectedType the expected result type
   * @param out a Ref that will hold the output cached column value, if we have
   * one. The contents of 'out' are not changed if this method returns false.
   * @return true if 'out' is populated with a cached result, false if we have
   * no cached result to use.
   */
  protected boolean lookupCache(int colIdx, Type expectedType, Ref<Object> out)
      throws ColumnParseException {
    // Check if we've cached a value for the column.
    if (mColumnValues.size() > colIdx) {
      // We may have cached a value for this column.
      Object cached = mColumnValues.get(colIdx);
      if (cached != null) {
        // Already parsed - return it!
        out.item = cached;
        return true;
      }

      // We got null from the cache; this may mean not-yet-parsed, or it
      // might be true null.
      if (mColumnNulls.get(colIdx)) {
        out.item = null; // True null.
        return true;
      }
    }

    // Check if we've cached a wrapper for the column bytes.
    CharBuffer cbCol = mColTexts.size() > colIdx ? mColTexts.get(colIdx) : null;

    if (null != cbCol) {
      // We have. Interpret the bytes inside.
      out.item = parseAndCache(cbCol, colIdx, expectedType);
      return true;
    }

    // No cached result.
    return false;
  }

  /**
   * @return the string representing a null string.
   */
  protected String getNullStr() {
    return mNullStr;
  }
}
