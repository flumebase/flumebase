// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.antlr.runtime.RecognitionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.QuerySubmitResponse;

import com.odiago.rtengine.lang.TypeChecker;
import com.odiago.rtengine.lang.VisitException;

import com.odiago.rtengine.parser.ASTGenerator;
import com.odiago.rtengine.parser.SQLStatement;

import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.PlanContext;

import com.odiago.rtengine.util.DAG;

/**
 * Standalone local execution environment for flows.
 */
public class LocalEnvironment extends ExecEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(
      LocalEnvironment.class.getName());

  /** The max number of events that will be processed in between polling for a control op. */
  private static final int MAX_STEPS = 250;

  static class ControlOp {
    enum Code {
      AddFlow,
      CancelFlow,
      CancelAll,
      ShutdownThread,
      Noop,
    };

    /** What operation should be performed by the worker thread? */
    private final Code mOpCode;

    /** What add'l data is required to do this operation? */
    private final Object mDatum;

    public ControlOp(Code opCode, Object datum) {
      mOpCode = opCode;
      mDatum = datum;
    }

    public Code getOpCode() {
      return mOpCode;
    }

    public Object getDatum() {
      return mDatum;
    }
  }


  /**
   * The thread where the active flows in the local environment actually operate.
   */
  private class LocalEnvThread extends Thread {
    /** The set of queues used to hold output from individual FlowElements. */
    private Collection<AbstractQueue<PendingEvent>> mAllFlowQueues;

    /** The set of running flows. */
    private Map<FlowId, LocalFlow> mActiveFlows;

    public LocalEnvThread() {
      mActiveFlows = new HashMap<FlowId, LocalFlow>();
      // TODO(aaron): This should be something like a heap, that keeps
      // the "most full" queues near the front of the line.
      mAllFlowQueues = new ArrayList<AbstractQueue<PendingEvent>>();
    }

    private void deployFlow(LocalFlow newFlow) {
      // Open all FlowElements in the flow, in reverse bfs order
      // (so sinks are always ready before sources). Add the output
      // queue(s) from the FlowElement to the set of output queues
      // we monitor for further event processing.
      newFlow.reverseBfs(new DAG.Operator<FlowElementNode>() {
        public void process(FlowElementNode elemNode) {
          FlowElement flowElem = elemNode.getFlowElement();

          // All FlowElements that we see will have LocalContext subclass contexts.
          // Get the output queue from this.
          LocalContext elemContext = (LocalContext) flowElem.getContext();
          elemContext.initControlQueue(mControlQueue);
          mAllFlowQueues.add(elemContext.getPendingEventQueue());

          try {
            flowElem.open();
          } catch (IOException ioe) {
            // TODO(aaron): Do something more drastic to prevent the flow from
            // being deployed.
            LOG.error("IOException when opening flow element: " + ioe);
          } catch (InterruptedException ie) {
            // TODO(aaron): Do something more drastic to prevent the flow from
            // being deployed.
            LOG.error("InterruptedException when opening flow element: " + ie);
          }
        }
      });
      mActiveFlows.put(newFlow.getId(), newFlow);
    }

    private void cancelFlowInner(LocalFlow flow) {
      // Close all FlowElements in the flow, and remove their output queues
      // from the set of queues we track.
      flow.bfs(new DAG.Operator<FlowElementNode>() {
        public void process(FlowElementNode elemNode) {
          FlowElement flowElem = elemNode.getFlowElement();
          try {
            flowElem.close();
          } catch (IOException ioe) {
            LOG.error("IOException when closing flow element: " + ioe);
          } catch (InterruptedException ie) {
            LOG.error("InterruptedException when closing flow element: " + ie);
          }

          // All FlowElements that we see will have LocalContext subclass contexts.
          // Get the output queue from this, and remove it from the tracking set.
          LocalContext elemContext = (LocalContext) flowElem.getContext();
          mAllFlowQueues.remove(elemContext.getPendingEventQueue());
        }
      });
    }

    private void cancelFlow(FlowId id) {
      LOG.info("Closing flow: " + id);
      LocalFlow flow = mActiveFlows.get(id);
      if (null == flow) {
        LOG.error("Cannot cancel flow: No flow available for id: " + id);
        return;
      }
      cancelFlowInner(flow);
      mActiveFlows.remove(id);
    }

    private void cancelAllFlows() {
      LOG.info("Closing all flows");
      Set<Map.Entry<FlowId, LocalFlow>> flowSet = mActiveFlows.entrySet();
      Iterator<Map.Entry<FlowId, LocalFlow>> flowIter = flowSet.iterator();
      while (flowIter.hasNext()) {
        Map.Entry<FlowId, LocalFlow> entry = flowIter.next();
        cancelFlowInner(entry.getValue());
      }

      mActiveFlows.clear();
    }

    @Override
    public void run() {
      boolean isFinished = false;

      while (true) {
        ControlOp nextOp = null;
        try {
          // Read the next operation from mControlQueue.
          nextOp = mControlQueue.take();
        } catch (InterruptedException ie) {
          // Expected; interruption here is used to notify us that we're done, etc.
        }

        if (null != nextOp) {
          switch (nextOp.getOpCode()) {
          case AddFlow:
            LocalFlow newFlow = (LocalFlow) nextOp.getDatum();
            deployFlow(newFlow);
            break;
          case CancelFlow:
            FlowId cancelId = (FlowId) nextOp.getDatum();
            cancelFlow(cancelId);
            break;
          case CancelAll:
            cancelAllFlows();
            break;
          case ShutdownThread:
            isFinished = true;
            break;
          case Noop:
            // Don't do any control operation; skip ahead to event processing.
            break;
          }
        }

        if (isFinished) {
          // Stop immediately; ignore any further event processing or control work.
          break;
        }

        // Check to see if there is work to be done inside the existing flows.
        // If we have more events to process, continue to do so. Every MAX_STEPS
        // events, check to see if we have additional control operations to perform.
        // If so, loop back around and give them an opportunity to process. If we
        // definitely have more event processing to do, but no control ops to
        // process, run for another MAX_STEPS.
        boolean continueEvents = true;
        while (continueEvents) {
          int processedEvents = 0;
          boolean processedEventThisIteration = false;
          for (AbstractQueue<PendingEvent> queue : mAllFlowQueues) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Dequeuing from queue: " + queue + " with size=" + queue.size());
            }
            while (!queue.isEmpty()) {
              PendingEvent pe = queue.remove();
              FlowElement fe = pe.getFlowElement();
              Event e = pe.getEvent();
              try {
                fe.takeEvent(e);
              } catch (IOException ioe) {
                // TODO(aaron): Encountering an exception mid-flow should cancel the flow.
                LOG.error("Flow element encountered IOException: " + ioe);
              } catch (InterruptedException ie) {
                LOG.error("Flow element encountered InterruptedException: " + ie);
              }
              processedEvents++;
              processedEventThisIteration = true;
              if (processedEvents > MAX_STEPS) {
                if (mControlQueue.size() > 0) {
                  // There are control operations to perform. Drain these.
                  continueEvents = false;
                } else {
                  // Run for another MAX_STEPS before checking again.
                  processedEvents = 0;
                }
              }
            }
          }

          if (!processedEventThisIteration) {
            // If we didn't process any events after scanning all queues,
            // then don't loop back around.
            break;
          }
        }
      }
    }
  }

  /** Next flow id to assign to new flows. */
  private long mNextFlowId;

  /** The AST generator used to parse user queries. */ 
  private ASTGenerator mGenerator;

  private static final int MAX_QUEUE_LEN = 100;

  private LocalEnvThread mLocalThread;
  private BlockingQueue<ControlOp> mControlQueue;

  public LocalEnvironment() {
    mGenerator = new ASTGenerator();
    mNextFlowId = 0;
    mControlQueue = new ArrayBlockingQueue<ControlOp>(MAX_QUEUE_LEN);
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
  public QuerySubmitResponse submitQuery(String query) throws InterruptedException {
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
  public FlowId addFlow(FlowSpecification spec) throws InterruptedException {
    if (null != spec) {
      // Turn the specification into a physical plan and run it.
      FlowId flowId = new FlowId(mNextFlowId++);
      LocalFlowBuilder flowBuilder = new LocalFlowBuilder(flowId);
      spec.reverseBfs(flowBuilder);
      LocalFlow localFlow = flowBuilder.getLocalFlow();
      mControlQueue.put(new ControlOp(ControlOp.Code.AddFlow, localFlow));
      return flowId;
    } else {
      return null;
    }
  }

  @Override
  public void cancelFlow(FlowId id) throws InterruptedException, IOException {
    mControlQueue.put(new ControlOp(ControlOp.Code.CancelFlow, id));
  }

  /**
   * Stop the local environment and shut down any flows operating therein.
   */
  @Override
  public void disconnect() throws InterruptedException {
    mControlQueue.put(new ControlOp(ControlOp.Code.CancelAll, null));
    mControlQueue.put(new ControlOp(ControlOp.Code.ShutdownThread, null));
    mLocalThread.join();
  }

}

