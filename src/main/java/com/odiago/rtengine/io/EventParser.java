// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.io;

import java.io.IOException;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.lang.Type;

/**
 * An EventParser defines the manner in which an input event -- a set of bytes
 * with an internal structure -- is parsed into the set of typed fields expected
 * by the rest of the stream processing system.
 *
 * <p>EventParser implementations are stateful and should be as lazy as
 * possible.  The runtime tries to call the <tt>getColumn()</tt> method for
 * only the columns required to satisfy a query; the ideal EventParser will
 * examine no more bytes of the underlying message than are necessary to
 * retrieve the columns in question. Partially-completed work should be
 * memoized in case a subsequent <tt>getColumn()</tt> request can make use of
 * it. When processing of the current event is complete, the
 * <tt>EventParser</tt> may be reused to process subsequent events, after
 * calling <tt>reset()</tt>.  </p>
 */
public abstract class EventParser {

  /**
   * Resets the internal state of the EventParser, and (re)initializes it such
   * that subsequent processing should be performed on the specified event
   * 'e'.
   */
  public abstract void reset(Event e);

  /**
   * Within the event specified at the last call to reset(), grab the value of
   * the column specified by colIdx (A 0-based index into the columns of the
   * object), and return it as an Object of the appropriate type, as specified
   * by the expectedType parameter.
   *
   * <p>Expected Java types, given an RTSQL type:</p>
   * <ul>
   *   <li>INT - Integer</li>
   *   <li>BIGINT - Long</li>
   *   <li>FLOAT - Float</li>
   *   <li>DOUBLE - Double</li>
   *   <li>STRING - String</li>
   *   <li>TIMESTAMP - java.sql.Timestamp</li>
   *   <li>TIMESPAN - Long</li>
   * </ul>
   *
   * @throws ColumnParseException if the column cannot be parsed according to
   * the specified type; either its does not exist, or it is in an unexpected
   * form and cannot be interpreted as an instance of the expected type. The
   * handling of missing columns, default values, etc. is handled outside the
   * EventParser; it is not the responsibility of the EventParser to return null
   * for a missing column that is nullable.
   */
  public abstract Object getColumn(int colIdx, Type expectedType)
      throws ColumnParseException, IOException;

}
