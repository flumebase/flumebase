// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

/**
 * FlowElement that advances events whose contents, when treated as a UTF-8 string,
 * match the supplied comparison string.
 * TODO(aaron): Operate on a field, not the raw bytes, which are assuredly avro-encoded...
 */
public class StrMatchFilterElement extends FlowElementImpl {
  /** The string that needs to be matched by the input event. */
  private String mMatchText;

  public StrMatchFilterElement(FlowElementContext ctxt, String match) {
    super(ctxt);
    mMatchText = match;
  }


  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    byte [] body = e.getEvent().getBody();
    String bodyStr = new String(body);

    if (mMatchText.equals(bodyStr)) {
      emit(e);
    }
  }

  @Override
  public String toString() {
    return "StrMatchFilter[matchText=\"" + mMatchText + "\"]";
  }
}
