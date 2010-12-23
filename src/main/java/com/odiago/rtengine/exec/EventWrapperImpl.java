// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

/**
 * Contains some methods of use to most single-Event EventWrapper implementations.
 */
public abstract class EventWrapperImpl extends EventWrapper {
  /** {@inheritDoc} */
  @Override
  public String getAttr(String attrName) {
    byte[] bytes = getEvent().getAttrs().get(attrName);
    if (null == bytes) {
      return null;
    } else {
      return new String(bytes);
    }
  }
}
