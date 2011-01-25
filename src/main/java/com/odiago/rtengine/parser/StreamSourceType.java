// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Defines the type of stream being created in a CreateStreamStmt.
 */
public enum StreamSourceType {
  File, // Data replayed from a file (local, or in HDFS).
  Sink, // Data pulled from a Flume sink.
  Memory, // Events already cached in local memory (used only for internal tests).
  Node, // Data pulled from a Flume logical node.
}
