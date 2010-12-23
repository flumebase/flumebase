// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import jline.ConsoleReader;
import jline.History;

/**
 * Tracks user command-line input to provide a history buffer that persists
 * across user sessions.
 */
public class HistoryFile {

  /** Default filename to use for this if unspecified. */
  private static final String DEFAULT_FILE_NAME = ".rtsql_history";

  private String mFileName;

  private BufferedWriter mWriter;

  public HistoryFile() {
    this(getDefaultFileName());
  }

  public HistoryFile(String filename) {
    mFileName = filename;
  }

  /**
   * Opens the history file for writing. logCommand() operations
   * are valid after this point.
   */
  public void open() throws IOException {
    mWriter = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(mFileName, true)));
  }

  /**
   * Closes the history file.
   */
  public void close() throws IOException {
    mWriter.close();
  }

  /**
   * Writes the specified line to the end of the history file.
   */
  public void logCommand(String line) throws IOException {
    mWriter.write(line);
    if (!line.endsWith("\n")) {
      mWriter.write("\n");
    }
  }

  /**
   * Opens the history file and reads in the lines, initializing the history
   * of the specified ConsoleReader.
   */
  public void populateConsoleReader(ConsoleReader conReader) throws IOException {
    File f = new File(mFileName);
    if (!f.exists()) {
      // Nothing to initialize.
      return;
    }

    History h = conReader.getHistory();
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(f)));
    try {
      while (true) {
        String line = br.readLine();
        if (null == line) {
          return;
        }

        h.addToHistory(line.trim());
      }
      
    } finally {
      br.close();
    }
  }


  /**
   * @return the filename we should use by default.
   */
  private static String getDefaultFileName() {
    String userHomeDir = System.getProperty("user.home");
    if (null == userHomeDir) {
      return DEFAULT_FILE_NAME; // Just use the default filename in the cwd.
    }

    // Return the default filename in the home directory.
    File homeDir = new File(userHomeDir);
    return new File(homeDir, DEFAULT_FILE_NAME).getAbsolutePath();
  }
}
