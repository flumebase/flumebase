// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;

import com.cloudera.flume.core.EventSink;

import com.cloudera.util.Pair;

/**
 * Plugin class for Flume that registers our sink with the Flume
 * configuration language.
 */
public class FlumePlugin {
  private static class RtsqlSinkBuilder extends SinkBuilder {
    /** {@inheritDoc} */
    @Override
    public EventSink build(Context ctxt, String... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("usage: rtsqlsink(\"flow/source\")");
      }

      String flowSourceId = args[0];
      return new RtsqlSink(flowSourceId);
    }
  }

  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("rtsqlsink", new RtsqlSinkBuilder()));
    return builders;
  }
}
