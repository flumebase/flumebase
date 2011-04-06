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
import java.util.Map;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.lang.PreciseType;
import com.odiago.flumebase.lang.Timestamp;
import com.odiago.flumebase.lang.Type;

/**
 * EventParser implementation that uses a delimiter character in between fields.
 * The delimiter character cannot appear in the fields themselves;
 * this does not support any enclosed- or escaped-by characters.
 */
public class DelimitedEventParser extends EventParser {

  private static final Logger LOG = LoggerFactory.getLogger(
      DelimitedEventParser.class.getName());

  /** key in the stream properties map specifying the field delimiter. */
  public static final String DELIMITER_PARAM = "delimiter";
  public static final char DEFAULT_DELIMITER = ',';

  /** key in the stream properties map specifying an escape to signify a null string. */
  public static final String NULL_STR_PARAM = "null.sequence";
  public static final String DEFAULT_NULL_STR = "\\N";

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

  /** An escape sequence that specifies that the current field is a null string. */
  private String mNullStr;

  /** Index of the field we will walk across next. */ 
  private int mCurField;

  /** CharBuffers wrapping the text of each column. */
  private ArrayList<CharBuffer> mColTexts;

  /** The reified instances of the columns in their final types. A null
   * here may mean 'uncached', or true null, if mColumnNulls[i] is true. */
  private ArrayList<Object> mColumnValues;

  /** Array of t/f values; if true, indicates that we parsed the column
   * in question, but determined the value to be null. */
  private ArrayList<Boolean> mColumnNulls;
  

  public DelimitedEventParser() {
    this(DEFAULT_DELIMITER);
  }

  public DelimitedEventParser(char delimiter) {
    mDelimiter = delimiter;
    mNullStr = DEFAULT_NULL_STR;
    init();
  }

  public DelimitedEventParser(Map<String, String> params) {
    // Set the delimiter character.
    String delimStr = params.get(DELIMITER_PARAM);
    if (null == delimStr || delimStr.length() == 0) {
      mDelimiter = DEFAULT_DELIMITER;
    } else {
      mDelimiter = delimStr.charAt(0);
    }

    // Specify a char sequence that represents a null string.
    mNullStr = params.get(NULL_STR_PARAM);
    if (null == mNullStr) {
      mNullStr = DEFAULT_NULL_STR;
    }

    init();
  }

  private void init() {
    mColTexts = new ArrayList<CharBuffer>();
    mColumnValues = new ArrayList<Object>();
    mColumnNulls = new ArrayList<Boolean>();
  }

  /** Clear all internal state and reset to a new unparsed event body. */
  @Override
  public void reset(Event e) {
    mEvent = e;
    mAsUtf8 = new Utf8(mEvent.getBody());
    mAsCharacters = null;
    mIndex = 0;
    mCurField = 0;
    mColTexts.clear();
    mColumnValues.clear();
    mColumnNulls.clear();
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
    // Check if we've cached a value for the column.
    if (mColumnValues.size() > colIdx) {
      // We may have cached a value for this column.
      Object cached = mColumnValues.get(colIdx);
      if (cached != null) {
        // Already parsed - return it!
        return cached;
      }

      // We got null from the cache; this may mean not-yet-parsed, or it
      // might be true null.
      if (mColumnNulls.get(colIdx)) {
        return null; // True null.
      }
    }

    // Check if we've cached a wrapper for the column bytes.
    CharBuffer cbCol = mColTexts.size() > colIdx ? mColTexts.get(colIdx) : null;

    if (null != cbCol) {
      // We have. Interpret the bytes inside.
      return parseAndCache(cbCol, colIdx, expectedType);
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
        mColTexts.add(cbField);
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

      mColTexts.add(cbField); // Always add fields to the end of the list.
      mCurField++; // We've added another field.
      mIndex++; // Advance past the delimiter character.
    }

    // We have separated enough fields; this one's text is cached. Parse its
    // value and return it.
    return parseAndCache(cbField, colIdx, expectedType);
  }

  /**
   * Given a CharBuffer wrapping a field, return the field's value in the
   * type expected by the runtime. Before returning, cache it in the slot for
   * 'colIdx'.
   */
  private Object parseAndCache(CharBuffer chars, int colIdx, Type expectedType)
      throws ColumnParseException {
    Type.TypeName primitiveTypeName = expectedType.getPrimitiveTypeName();

    String debugInputString = null;
    if (LOG.isDebugEnabled()) {
      // Save this for the end. This method may consume the chars object,
      // so we need to do this up-front.
      debugInputString = chars.toString();
    }

    // TODO(aaron): Test how this handles a field that is an empty string.
    Object out = null;
    switch (primitiveTypeName) {
    case BOOLEAN:
      out = CharBufferUtils.parseBool(chars);
      break;
    case INT:
      out = CharBufferUtils.parseInt(chars);
      break;
    case BIGINT:
      out = CharBufferUtils.parseLong(chars);
      break;
    case FLOAT:
      out = CharBufferUtils.parseFloat(chars);
      break;
    case DOUBLE:
      out = CharBufferUtils.parseDouble(chars);
      break;
    case STRING:
      String asStr = chars.toString();
      if (expectedType.isNullable() && asStr.equals(mNullStr)) {
        out = null;
      } else {
        out = asStr;
      }
      break;
    case TIMESTAMP:
      out = CharBufferUtils.parseLong(chars);
      if (null != out) {
        out = new Timestamp((Long) out);
      }
      break;
    case TIMESPAN:
      // TODO: This should return a TimeSpan object, which is actually two
      // fields. We need to work on this... it should not just be a 'long'
      // representation.
      out = CharBufferUtils.parseLong(chars);
      break;
    case PRECISE:
      PreciseType preciseType = PreciseType.toPreciseType(expectedType);
      out = preciseType.parseStringInput(chars.toString());
      break;
    default:
      throw new ColumnParseException("Cannot parse recursive types");
    }

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

    //if (LOG.isDebugEnabled()) {
    //  LOG.debug("Parsed string [" + debugInputString + "] with expected type ["
    //      + expectedType + "] for column idx=" + colIdx + "; result is [" + out + "]"); 
    //}
    return out;
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
