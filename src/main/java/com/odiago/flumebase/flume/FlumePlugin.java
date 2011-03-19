// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.flume;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;

import com.cloudera.util.Pair;

import com.odiago.flumebase.util.StringUtils;

/**
 * Plugin class for Flume that registers our sink with the Flume
 * configuration language.
 */
public class FlumePlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      FlumePlugin.class.getName());

  private static class RtsqlSourceBuilder extends SourceBuilder {
    /** {@inheritDoc} */
    public EventSource build(Context ctxt, String... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("usage: rtsqlsource(\"flow/source\")");
      }

      String outputContextId = args[0];
      return new RtsqlSource(outputContextId);
    }
  }

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

  private static class RtsqlMultiSinkBuilder extends SinkBuilder {
    /** {@inheritDoc} */
    @Override
    public EventSink build(Context ctxt, String... args) {
      if (args.length != 1) {
        throw new IllegalArgumentException("usage: rtsqlmultisink(\"id\")");
      }

      String portId = args[0];
      RtsqlMultiSink existingMultiSink = RtsqlMultiSink.getMultiSinkInstance(portId);
      if (null != existingMultiSink) {
        // Just use the existing instance.
        LOG.debug("Recycling existing RtsqlMultiSink for portId=" + portId);
        return existingMultiSink;
      } else {
        try {
          return new RtsqlMultiSink(portId);
        } catch (IOException ioe) {
          LOG.error("IOException creating multisink: " + StringUtils.stringifyException(ioe));
          return null;
        }
      }
    }
  }

  public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
    List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
    builders.add(new Pair<String, SourceBuilder>("rtsqlsource", new RtsqlSourceBuilder()));
    return builders;
  }

  public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
    List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
    builders.add(new Pair<String, SinkBuilder>("rtsqlsink", new RtsqlSinkBuilder()));
    builders.add(new Pair<String, SinkBuilder>("rtsqlmultisink", new RtsqlMultiSinkBuilder()));
    return builders;
  }
}
