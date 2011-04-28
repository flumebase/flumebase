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

import java.util.Map;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.util.Ref;

/**
 * EventParser implementation that uses a delimiter character in between fields.
 * The delimiter character cannot appear in the fields themselves;
 * this does not support any enclosed- or escaped-by characters.
 */
public class DelimitedEventParser extends CachingTextEventParser {

  private static final Logger LOG = LoggerFactory.getLogger(
      DelimitedEventParser.class.getName());

  /** key in the stream properties map specifying the field delimiter. */
  public static final String DELIMITER_PARAM = "delimiter";
  public static final char DEFAULT_DELIMITER = ',';

  /** The event we're processing. */
  private Event mEvent;

  /** A UTF-8 wrapper around the event's bytes. */
  private Utf8 mAsUtf8;

  /** A char array representation converted from the event's bytes. */
  private char [] mAsCharacters;

  /** The current cursor index into mAsCharacters. */
  private int mIndex;

  /** The delimiter character we're using. */
  private char mDelimiter;

  /** Index of the field we will walk across next. */ 
  private int mCurField;

  public DelimitedEventParser() {
    this(DEFAULT_DELIMITER);
  }

  public DelimitedEventParser(char delimiter) {
    super();
    mDelimiter = delimiter;
  }

  public DelimitedEventParser(Map<String, String> params) {
    super(params);
    // Set the delimiter character.
    String delimStr = params.get(DELIMITER_PARAM);
    if (null == delimStr || delimStr.length() == 0) {
      mDelimiter = DEFAULT_DELIMITER;
    } else {
      mDelimiter = delimStr.charAt(0);
    }
  }

  /** Clear all internal state and reset to a new unparsed event body. */
  @Override
  public void reset(Event e) {
    super.reset(e);
    mEvent = e;
    mAsUtf8 = new Utf8(mEvent.getBody());
    mAsCharacters = null;
    mIndex = 0;
    mCurField = 0;
  }

  /**
   * Return the value of the colIdx'th column in the expected type form.
   *
   * <p>
   * First, check if we've already cached the value. If so, return it.
   * Next, check if we've cached a CharBuffer that wraps the underlying text.
   * If so, convert that to the correct value, cache it, and return it.
   * Finally, walk forward from our current position in mAsCharacters,
   * looking for delimiters. As we find delimiters, mark and cache the
   * discovered columns in mColTexts. When we arrive at the column of
   * interest, cache and return its value.
   * </p>
   */
  @Override
  public Object getColumn(int colIdx, Type expectedType) throws ColumnParseException {

    // Check if we've cached this column's value, or a wrapper for the column bytes.
    Ref<Object> cachedResult = new Ref<Object>();
    if (lookupCache(colIdx, expectedType, cachedResult)) {
      // Yes; return the cached result.
      return cachedResult.item;
    }

    // Check if we have yet decoded the UTF-8 bytes of the event into a char
    // array.
    if (mAsCharacters == null) {
      // Nope, do so now.
      // TODO(aaron): Make sure we're not making an additional copy. The
      // toString() call converts the bytes into a String; Does String.toCharArray()
      // then make an additional copy of the backing array? Can we do better?
      mAsCharacters = mAsUtf8.toString().toCharArray();
    }

    // While we have to walk more fields to get the one we need...
    CharBuffer cbField = null;
    while (mCurField <= colIdx) {
      // We have to continue walking through the underlying string.
      int start = mIndex; // The field starts here.
      if (start == mAsCharacters.length && mCurField == colIdx
          && expectedType.getPrimitiveTypeName().equals(Type.TypeName.STRING)) {
        // We have found an empty string field at the end of the record.
        // Return the empty string as a field.
        cbField = CharBuffer.wrap("");
        cacheColText(cbField);
        mCurField++;
        mIndex++;
        return parseAndCache(cbField, colIdx, expectedType);
      } else if (start >= mAsCharacters.length) {
        // We don't have any more fields we can parse. If we need to read
        // more fields, then this is an error; the event is too short.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Not enough fields: mCurField=" + mCurField + " and no more string left");
        }

        throw new ColumnParseException("Not enough fields");
      }

      for ( ; mIndex < mAsCharacters.length; mIndex++) {
        char cur = mAsCharacters[mIndex];
        if (mDelimiter == cur) {
          // Found the end of the current field.
          break;
        }
      }

      // We have ended the current field, either by finding its delimiter, or hitting
      // the end of the entire record. Wrap this field in a character buffer, and
      // memoize it.
      int delimPos = mIndex;
      cbField = CharBuffer.wrap(mAsCharacters, start, delimPos - start);

      cacheColText(cbField); // Always add fields to the end of the list.
      mCurField++; // We've added another field.
      mIndex++; // Advance past the delimiter character.
    }

    // We have separated enough fields; this one's text is cached. Parse its
    // value and return it.
    return parseAndCache(cbField, colIdx, expectedType);
  }

  @Override
  public String toString() {
    return "DelimitedEventParser(delimiter=" + mDelimiter + ")";
  }

  @Override
  public boolean validate(StreamSymbol streamSym) {
    // The delimited event parser can handle virtually anything; it also has
    // default values for delimiters, etc. so there's no need for a user config
    // to be matched against the stream.
    return true;
  }
}
