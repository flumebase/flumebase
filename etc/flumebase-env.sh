#!/bin/bash
#
# Licensed to Odiago, Inc. under one or more contributor license
# agreements.  See the NOTICE.txt file distributed with this work for
# additional information regarding copyright ownership.  Odiago, Inc.
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.

# This configuration file sets environment variables necessary for FlumeBase to run.

####################################################################
# Paths controlling FlumeBase operation
####################################################################

# Where should a FlumeBase server write logs to?
export FLUMEBASE_LOG_DIR=${FLUMEBASE_LOG_DIR:-${FLUMEBASE_HOME}/logs}

# Where should server pid files be stored?
export FLUMEBASE_PID_DIR=${FLUMEBASE_PID_DIR:-${FLUMEBASE_HOME}/pids}


####################################################################
# Paths identifying locations of third-party dependencies
####################################################################

# Location of Flume? Default to CDH installation target.
export FLUME_HOME=${FLUME_HOME:-/usr/lib/flume}



####################################################################
# Options controlling FlumeBase operation
####################################################################

# What options should be passed to the JVM when executing FlumeBase?
export FLUMEBASE_OPTS=${FLUMEBASE_OPTS:-}

# What initial entries should be placed on the FlumeBase classpath?
# Anything in $FLUMEBASE_HOME/lib will automatically be added.
export FLUMEBASE_CLASSPATH=${FLUMEBASE_CLASSPATH:-}

# Control maximum memory consumption of a FlumeBase server. Default 1000 MB.
export FLUMEBASE_HEAP_SIZE=${FLUMEBASE_HEAP_SIZE:-1000}

