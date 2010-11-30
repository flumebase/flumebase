// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.List;

import org.apache.avro.Schema;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that prints events to the console.
 */
public class ConsoleOutputElement extends FlowElementImpl {
  private List<TypedField> mFields;

  public ConsoleOutputElement(FlowElementContext context, Schema inputSchema,
      List<TypedField> fields) {
    super(context);

    mFields = fields;
  }

  private void printHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("timestamp");
    for (TypedField field : mFields) {
      sb.append("\t");
      sb.append(field.getName());
    }
    System.out.println(sb);
  }

  @Override
  public void open() {
    printHeader();
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
    System.out.println(sb.toString());
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
