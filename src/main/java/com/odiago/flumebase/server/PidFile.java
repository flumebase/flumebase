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

package com.odiago.flumebase.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.util.StringUtils;

/**
 * Maintain a PID file for the lifetime of this program.
 */
public class PidFile {
  private static final Logger LOG = LoggerFactory.getLogger(PidFile.class.getName());

  private File mFinalFile; // "final" file that is shared by all potential process instances.
  private File mTempFile; // "temp" filename used only by processes with this same pid.

  public PidFile() {
  }

  /**
   * Acquire the pid file lock.
   * @return true if successful, false otherwise.
   */
  public boolean acquire() {
    boolean success = false;
    String pidDir = System.getenv("FLUMEBASE_PID_DIR");
    if (null == pidDir) {
      LOG.error("Environment variable not set: FLUMEBASE_PID_DIR");
      return false; // No pid directory set.
    }

    File pidDirFile = new File(pidDir);
    if (!pidDirFile.exists()) {
      pidDirFile.mkdirs();
    }

    if (!pidDirFile.exists()) {
      // Couldn't make the pid dir and it was missing.
      LOG.error("Couldn't make pid dir: " + pidDirFile.getAbsolutePath());
      return false;
    }

    mFinalFile = new File(pidDirFile, "flumebase.pid");
    if (mFinalFile.exists()) {
      // Pid file already exists.
      LOG.error("Pid file " + mFinalFile.getAbsolutePath() + " already exists");
      return false;
    }

    BufferedReader r = null;
    InputStream is = null;
    BufferedWriter w = null;
    OutputStream os = null;
    try {
      // Execute a process that gets its parent process' id (our pid).
      String [] pidArgs = { "bash", "-c", "echo $PPID" };
      Process p = Runtime.getRuntime().exec(pidArgs);
      is = p.getInputStream();
      r = new BufferedReader(new InputStreamReader(is));
      int exitVal = p.waitFor();
      if (0 != exitVal) {
        LOG.error("Getting ppid had exit value " + exitVal);
        return false;
      }

      String pid = r.readLine();
      if (null == pid) {
        LOG.error("null pid?");
        return false;
      }

      pid = pid.trim();
      if (pid.length() == 0) {
        LOG.error("Empty pid?");
        return false;
      }

      // Write to a temporary pid file.
      mTempFile = new File(pidDirFile, "flumebase.pid." + pid);
      os = new FileOutputStream(mTempFile);
      w = new BufferedWriter(new OutputStreamWriter(os));
      w.write(pid);
      w.close();
      w = null;

      // Now hardlink it in place!

      p = Runtime.getRuntime().exec("ln " + mTempFile.getAbsolutePath() + " "
          + mFinalFile.getAbsolutePath());
      exitVal = p.waitFor();
      if (0 != exitVal) {
        LOG.error("Lost race while creating pid file. Another FlumeBase already started?");
        return false;
      }

      LOG.info("Got pidfile lock for pid " + pid);
      success = true;
    } catch (InterruptedException ie) {
      LOG.error("Interrupted waiting for subprocess");
      return false;
    } catch (IOException ioe) {
      LOG.error("IOException getting ppid: " + StringUtils.stringifyException(ioe));
      // Didn't work...
      return false;
    } finally {
      if (null != r) {
        try {
          r.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing process stream: " + ioe);
        }
      }

      if (null != w) {
        try {
          w.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing output stream: " + ioe);
        }
      }

      if (success) {
        // If we're definitely successful, make sure we delete these files on exit.
        mFinalFile.deleteOnExit();
        mTempFile.deleteOnExit();
      }
    }

    // Everything seemed to go successfully.
    return true;
  }
}
