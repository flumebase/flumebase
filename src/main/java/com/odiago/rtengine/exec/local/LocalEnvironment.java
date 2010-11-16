// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.antlr.runtime.RecognitionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.QuerySubmitResponse;

import com.odiago.rtengine.lang.TypeChecker;
import com.odiago.rtengine.lang.VisitException;

import com.odiago.rtengine.parser.ASTGenerator;
import com.odiago.rtengine.parser.SQLStatement;

import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.PlanContext;

/**
 * Standalone local execution environment for flows.
 */
public class LocalEnvironment extends ExecEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(
      LocalEnvironment.class.getName());

  /**
   * The thread where the active flows in the local environment actually operate.
   */
  private class LocalEnvThread extends Thread {
    public LocalEnvThread() {
    }

    @Override
    public void run() {
      // TODO(aaron): actually run things in this thread.
      while (true) {
        if (mIsFinished) {
          // We're done with the thread, stop immediately.
          LOG.debug("Got exit signal; closing local environment thread.");
          break;
        }

        try {
          // TODO(aaron): Remove this.
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted while sleeping in worker thread: " + ie);
        }
      }
    }
  }

  /** Next flow id to assign to new flows. */
  private long mNextFlowId;

  /** The AST generator used to parse user queries. */ 
  private ASTGenerator mGenerator;

  private LocalEnvThread mLocalThread;
  private volatile boolean mIsFinished;

  public LocalEnvironment() {
    mIsFinished = false;
    mGenerator = new ASTGenerator();
    mNextFlowId = 0;
    mLocalThread = this.new LocalEnvThread();
    mLocalThread.start();
  }

  @Override
  public String getEnvName() {
    return "local";
  }

  /**
   * Take the user's query, convert it into a local plan,
   * and execute it.
   */
  @Override
  public QuerySubmitResponse submitQuery(String query) {
    StringBuilder msgBuilder = new StringBuilder();
    FlowId flowId = null;
    try {
      // Send the parser's error messages into a buffer rather than stderr.
      ByteArrayOutputStream errBufferStream = new ByteArrayOutputStream();
      PrintStream errStream = new PrintStream(errBufferStream);

      SQLStatement stmt = mGenerator.parse(query, errStream);

      errStream.close();
      String errMsg = new String(errBufferStream.toByteArray());
      msgBuilder.append(errMsg);

      if (null == stmt) {
        msgBuilder.append("(Could not parse command)");
        return new QuerySubmitResponse(msgBuilder.toString(), null);
      }

      stmt.accept(new TypeChecker());
      PlanContext planContext = new PlanContext();
      stmt.createExecPlan(planContext);
      FlowSpecification spec = planContext.getFlowSpec();
      msgBuilder.append(planContext.getMsgBuilder().toString());
      flowId = addFlow(spec);
    } catch (VisitException ve) {
      msgBuilder.append("Error processing command: " + ve.getMessage());
    } catch (RecognitionException re) {
      msgBuilder.append("Error parsing command: " + re.getMessage());
    }

    return new QuerySubmitResponse(msgBuilder.toString(), flowId);
  }

  @Override
  public FlowId addFlow(FlowSpecification spec) {
    if (null != spec) {
      // TODO(aaron): Turn the specification into a physical plan and run it.
      LOG.info("Got a specification, but don't know how to make the work.");
      return new FlowId(mNextFlowId++);
    } else {
      return null;
    }
  }

  @Override
  public void cancelFlow(FlowId id) throws InterruptedException, IOException {
    throw new IOException("Not implemented: Cannot cancelFlow(" + id + ")");
  }

  /**
   * Stop the local environment and shut down any flows operating therein.
   */
  @Override
  public void disconnect() throws InterruptedException {
    mIsFinished = true;
    mLocalThread.join();
  }

}

