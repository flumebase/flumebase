// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.client;

import java.io.IOException;

import com.odiago.rtengine.util.QuitException;

import jline.ConsoleReader;

/**
 * Client frontend to rtengine system.
 */
public class CmdLineClient {
  private static final String VERSION_STRING = "rtengine 1.0.0 / rtsql for Flume";

  /**
   * True if we're mid-buffer on a command.
   */
  private boolean mInCommand;

  private StringBuilder mCmdBuilder;

  /**
   * Print the version string to stdout.
   */
  private void printVersion() {
    System.out.println(VERSION_STRING);
  }

  private void printUsage() {
    System.out.println("");
    System.out.println("All text commands must end with a ';' character.");
    System.out.println("Session control commands must be on a line by themselves.");
    System.out.println("");
    System.out.println("Session control commands:");
    System.out.println("  \\c  Cancel the current input statement.");
    System.out.println("  \\h  Print help message.");
    System.out.println("  \\q  Quit the client.");
    System.out.println("");
  }

  /**
   * Reset the current command buffer.
   */
  private void resetCmdState() {
    mInCommand = false;
    mCmdBuilder = new StringBuilder();
  }

  /**
   * Handle a '\x' event for various values of the escape character 'x'.
   */
  private void handleEscape(char escapeChar) throws QuitException {
    switch(escapeChar) {
    case 'c':
      resetCmdState();
      break;
    case 'h':
      printUsage();
      break;
    case 'q':
      // Graceful quit from the shell.
      throw new QuitException(0);
    }
  }

  /**
   * Given a command which may contain leading or trailing whitespace
   * and a final ';' at the end, remove these unnecessary characters.
   */
  private String trimTerminator(String cmdIn) {
    String trimmed = cmdIn.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }

    return trimmed;
  }
  /**
   * Parse and execute a text command.
   */
  private void execCommand(String cmd) throws IOException, QuitException {
    String realCommand = trimTerminator(cmd);
    
    if (realCommand.equalsIgnoreCase("help")) {
      printUsage();
    } else if (realCommand.equalsIgnoreCase("quit")) {
      throw new QuitException(0);
    } else {
      System.err.println("Unknown command: " + cmd);
    }
  }

  private void printPrompt() {
    if (mInCommand) {
      System.out.print("    -> ");
    } else {
      System.out.print("rtsql> ");
    }
  }

  /**
   * Main entry point for the command-line client.
   * @return the exit code for the program (0 on success).
   */
  public int run() throws IOException {
    System.out.println("Welcome to the rtengine client.");
    printVersion();
    System.out.println("Type 'help;' or '\\h' for instructions.");
    System.out.println("Type 'exit;' or '\\q' to quit.");

    ConsoleReader conReader = new ConsoleReader();

    mInCommand = false;
    mCmdBuilder = new StringBuilder();

    try {
      while (true) {
        printPrompt();
        String line = conReader.readLine();
        if (null == line) {
          // EOF on input. We're done.
          throw new QuitException(0);
        }
        String trimmed = line.trim();
        if (trimmed.startsWith("\\")) {
          handleEscape(line.charAt(1));
          resetCmdState();
        } else if (trimmed.endsWith(";")) {
          mCmdBuilder.append(line);
          mCmdBuilder.append('\n');
          execCommand(mCmdBuilder.toString());
          resetCmdState();
        } else {
          mCmdBuilder.append(line);
          mCmdBuilder.append('\n');
          mInCommand = true;
        }
      }
    } catch (QuitException qe) {
      System.out.println("Goodbye");
      return qe.getStatus();
    }
  }

  public static void main(String [] args) throws Exception  {
    CmdLineClient client = new CmdLineClient();
    System.exit(client.run());
  }
}
