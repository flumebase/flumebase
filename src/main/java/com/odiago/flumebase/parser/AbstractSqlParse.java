// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import java.io.PrintStream;

import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

/**
 * Base parse tree representing a RTSQL statement.
 */
public abstract class AbstractSqlParse extends Parser {
  /** Where do we send error information? */
  private PrintStream mErrPrintStream = System.err;

  protected AbstractSqlParse(TokenStream input) {
    super(input);
  }

  protected AbstractSqlParse(TokenStream input, RecognizerSharedState state) {
    super(input, state);
  }

  /**
   * Override the default behavior (write directly to stderr) with behavior
   * that allows us to write to the configured stream buffer of our choice.
   */
  @Override
  public void emitErrorMessage(String msg) {
    mErrPrintStream.println(msg);
  }

  /**
   * Specify the output stream where error messages are written.
   */
  public void setErrorStream(PrintStream errStream) {
    mErrPrintStream = errStream;
  }

  /**
   * Remove "double quotes" from around a string, if present.
   */
  protected final String unquote(String srcName) {
    if ((srcName.startsWith("\"") && srcName.endsWith("\""))
        || (srcName.startsWith("\'") && srcName.endsWith("\'"))) {
      return srcName.substring(1, srcName.length() - 1);
    } else {
      return srcName;
    }
  }

  /**
   * Given an input string which was of the form "some text" or 'some text',
   * where the inner text may include escape characters, remove the enclosing
   * [double] quotes and transform any escape sequences into their respective
   * proper characters.
   */
  protected final String unescape(String inputStr) {
    inputStr = unquote(inputStr);
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < inputStr.length(); i++) {
      char c = inputStr.charAt(i);
      if (c == '\\') {
        // We've found an escape sequence.
        i++;
        if (i == inputStr.length()) {
          // We found a lone '\' by itself at the end of the string.
          // Just include it literally.
          sb.append('\\');
        } else {
          char esc = inputStr.charAt(i);
          switch (esc) {
          case 'b':
            sb.append('\b');
            break;
          case 't':
            sb.append('\t');
            break;
          case 'n':
            sb.append('\n');
            break;
          case 'f':
            sb.append('\f');
            break;
          case 'r':
            sb.append('\r');
            break;
          case '\"':
            sb.append('\"');
            break;
          case '\'':
            sb.append('\'');
            break;
          case '\\':
            sb.append('\\');
            break;
          default:
            // Undefined escape characters resolve to themselves.
            sb.append(esc);
            break;
          }
        }
      } else {
        // normal character.
        sb.append(c);
      }
    }

    return sb.toString();
  }
}
