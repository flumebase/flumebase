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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.util.Ref;

/**
 * EventParser implementation that uses a regex to determine how to
 * extract fields from a string.
 *
 * This relies on a 'regex' property of the stream; each (group) in
 * the regex specifies a different column of the output.
 */
public class RegexEventParser extends CachingTextEventParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      RegexEventParser.class.getName());

  /** key in the stream properties map specifying the regex. */
  public static final String REGEX_PARAM = "regex";

  /** The event we're processing. */
  private Event mEvent;

  /** A UTF-8 wrapper around the event's bytes. */
  private Utf8 mAsUtf8;

  /** The regular expression we match against. */
  private String mRegexStr;

  /** The compiled regular expression to apply to each event. */
  private Pattern mRegexPattern;

  /** The match result of applying the regex to a string-based event body. */
  private Matcher mMatcher;

  /** The next field we will extract from the regex. */
  private int mCurField;

  public RegexEventParser(Map<String, String> params) {
    super(params);
    // Set the regex.
    mRegexStr = params.get(REGEX_PARAM);
    if (null != mRegexStr) {
      // TODO: Cache this mapping from string -> Pattern objects.
      mRegexPattern = Pattern.compile(mRegexStr);
    }
  }

  /** Clear all internal state and reset to a new unparsed event body. */
  @Override
  public void reset(Event e) {
    super.reset(e);
    mEvent = e;
    mAsUtf8 = new Utf8(mEvent.getBody());
    mCurField = 0;
    mMatcher = null;
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

    // Check if we have yet matched the UTF-8 bytes against the regex.
    assert null != mRegexPattern;
    if (null == mMatcher) {
      // Nope, do so now. Apply the regular expression to it.
      mMatcher = mRegexPattern.matcher(mAsUtf8);
      mMatcher.find(); // Align the matcher with the first matching instance in the event.
    }

    // While we have to walk more fields to get the one we need...
    CharBuffer cbField = null;
    while (mCurField <= colIdx) {
      // Keep extracting groups from the regex match.
      try {
        String fieldStr = mMatcher.group(1 + mCurField); // 1-based offset not 0-based.
        cbField = CharBuffer.wrap(fieldStr);
      } catch (IllegalStateException ise) {
        // Couldn't extract a group for this field. Wrap a null.
        if (expectedType.equals(Type.getNullable(Type.TypeName.STRING))) {
          cbField = CharBuffer.wrap(getNullStr());
        } else if (expectedType.isNullable()) {
          cbField = CharBuffer.wrap("");
        } else {
          throw new ColumnParseException(ise);
        }
      }

      cacheColText(cbField);
      mCurField++;
    }

    // We have separated enough fields; this one's text is cached. Parse its
    // value and return it.
    return parseAndCache(cbField, colIdx, expectedType);
  }

  @Override
  public String toString() {
    return "RegexEventParser(regex=" + mRegexStr + ")";
  }

  @Override
  public boolean validate(StreamSymbol streamSym) {
    // The RegexEventParser requires the 'regex' property of the stream to be configured.
    return mRegexStr != null;
  }
}
