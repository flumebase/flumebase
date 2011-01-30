// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.List;

import org.apache.avro.Schema;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.server.UserSession;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that prints events to the consoles of each subscriber.
 */
public class ConsoleOutputElement extends FlowElementImpl {
  private List<TypedField> mFields;

  public ConsoleOutputElement(FlowElementContext context, Schema inputSchema,
      List<TypedField> fields) {
    super(context);

    mFields = fields;
  }

  private StringBuilder formatHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("timestamp");
    for (TypedField field : mFields) {
      sb.append("\t");
      sb.append(field.getDisplayName());
    }
    return sb;
  }

  @Override
  public void onConnect(UserSession session) {
    // When a user first connects, print the header for our output columns.
    session.sendInfo(formatHeader().toString());
  }

  @Override
  public void open() {
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException {
    StringBuilder sb = new StringBuilder();
    long ts = e.getEvent().getTimestamp();
    sb.append(ts);

    // Extract the Avro record from the event.
    for (TypedField field : mFields) {
      sb.append('\t');
      Object fieldVal = e.getField(field);
      if (null == fieldVal) {
        sb.append("null");
      } else {
        sb.append(fieldVal);
      }
    }

    // Notify all subscribers of our output.
    LocalContext context = (LocalContext) getContext();
    String output = sb.toString();
    for (UserSession session : context.getFlowData().getSubscribers()) {
      session.sendInfo(output);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ConsoleOutput(");
    StringUtils.formatList(sb, mFields);
    sb.append(")");
    return sb.toString();
  }
}
