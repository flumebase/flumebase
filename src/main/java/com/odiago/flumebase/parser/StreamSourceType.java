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

package com.odiago.flumebase.parser;

/**
 * Defines the type of stream being created in a CreateStreamStmt.
 */
public enum StreamSourceType {
  File, // Data replayed from a file (local, or in HDFS).
  Source, // Data pulled from a Flume source.
  Memory, // Events already cached in local memory (used only for internal tests).
  Node, // Data pulled from a Flume logical node.
  Select, // The output of a persistent SELECT statement.
}
