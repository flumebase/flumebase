// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Input source that reads from a local file.
 */
public class FileInputNode extends PlanNode {
  private String mFilename;

  public FileInputNode(String filename) {
    mFilename = filename;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("FileInput filename=");
    sb.append(mFilename);
  }
}
