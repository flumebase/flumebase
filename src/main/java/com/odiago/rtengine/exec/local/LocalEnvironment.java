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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.antlr.runtime.RecognitionException;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.BuiltInSymbolTable;
import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.FlowInfo;
import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.QuerySubmitResponse;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.flume.EmbeddedFlumeConfig;

import com.odiago.rtengine.lang.AssignFieldLabelsVisitor;
import com.odiago.rtengine.lang.JoinKeyVisitor;
import com.odiago.rtengine.lang.JoinNameVisitor;
import com.odiago.rtengine.lang.TypeChecker;
import com.odiago.rtengine.lang.VisitException;

import com.odiago.rtengine.parser.ASTGenerator;
import com.odiago.rtengine.parser.SQLStatement;

import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PropagateSchemas;

import com.odiago.rtengine.util.DAG;
import com.odiago.rtengine.util.DAGOperatorException;
import com.odiago.rtengine.util.Ref;

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
      AddFlow,         // A new flow shold be deployed.
      CancelFlow,      // An entire flow should be canceled.
      CancelAll,       // All flows should be canceled.
      ShutdownThread,  // Stop processing anything else, immediately.
      Noop,            // Do no control action; just service data events.
      ElementComplete, // A flow element is complete and should be freed.
      Join,            // Add an object to the list of objects to be notified when a
                       // flow is canceled.
      ListFlows,       // Enumerate the running flows.
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
   * Container for information maintained by the local environment
   * regarding an active flow.
   */
  private static class ActiveFlowData {
    /** The flow itself. */
    private final LocalFlow mLocalFlow;

    /**
     * Set of objects which will have notify() called when the flow is
     * complete. The join target is a reference to a Boolean, which
     * will be set to True when the flow is complete.
     */
    private final List<Ref<Boolean>> mJoinTargets;

    public ActiveFlowData(LocalFlow flow) {
      mLocalFlow = flow;
      mJoinTargets = new ArrayList<Ref<Boolean>>();
    }

    public LocalFlow getFlow() {
      return mLocalFlow;
    }

    public FlowId getFlowId() {
      return mLocalFlow.getId();
    }

    /** Notifies everyone waiting on this flow that it is canceled. */
    public void cancel() {
      for (Ref<Boolean> joinTarget : mJoinTargets) {
        synchronized (joinTarget) {
          joinTarget.item = Boolean.TRUE;
          joinTarget.notify();
        }
      }
    }

    /** Waits for this flow to terminate. */
    public void subscribeToCancelation(Ref<Boolean> obj) {
      mJoinTargets.add(obj);
    }
  }

  /**
   * The thread where the active flows in the local environment actually operate.
   */
  private class LocalEnvThread extends Thread {
    /** The set of queues used to hold output from individual FlowElements. */
    private Collection<AbstractQueue<PendingEvent>> mAllFlowQueues;

    /** The set of running flows. */
    private Map<FlowId, ActiveFlowData> mActiveFlows;

    private boolean mFlumeStarted;

    public LocalEnvThread() {
      mActiveFlows = new HashMap<FlowId, ActiveFlowData>();
      // TODO(aaron): This should be something like a heap, that keeps
      // the "most full" queues near the front of the line.
      mAllFlowQueues = new ArrayList<AbstractQueue<PendingEvent>>();
      mFlumeStarted = false;
    }

    private void deployFlow(LocalFlow newFlow) throws IOException, InterruptedException {
      // If we haven't yet started Flume, and the flow requires Flume-based sources,
      // start Flume.
      if (newFlow.requiresFlume() && !mFlumeStarted) {
        mFlumeConfig.start();
        mFlumeStarted = true;
      }

      // Open all FlowElements in the flow, in reverse bfs order
      // (so sinks are always ready before sources). Add the output
      // queue(s) from the FlowElement to the set of output queues
      // we monitor for further event processing.
      try {
        newFlow.reverseBfs(new DAG.Operator<FlowElementNode>() {
          public void process(FlowElementNode elemNode) throws DAGOperatorException {
            FlowElement flowElem = elemNode.getFlowElement();

            // All FlowElements that we see will have LocalContext subclass contexts.
            // Get the output queue from this.
            LocalContext elemContext = (LocalContext) flowElem.getContext();
            elemContext.initControlQueue(mControlQueue);
            mAllFlowQueues.add(elemContext.getPendingEventQueue());

            try {
              flowElem.open();
            } catch (IOException ioe) {
              throw new DAGOperatorException(ioe);
            } catch (InterruptedException ie) {
              throw new DAGOperatorException(ie);
            }
          }
        });
      } catch (DAGOperatorException doe) {
        // This is a wrapper exception; unpack and rethrow with the appropriate type.
        Throwable cause  = doe.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else if (cause instanceof InterruptedException) {
          throw (InterruptedException) cause;
        } else {
          // Don't know how we got here. In any case, do not consider this
          // flow active.
          LOG.error("Unexpected DAG exception: " + doe);
          return;
        }
      }

      mActiveFlows.put(newFlow.getId(), new ActiveFlowData(newFlow));
    }

    private void cancelFlowInner(ActiveFlowData flowData) {
      // Close all FlowElements in the flow, and remove their output queues
      // from the set of queues we track.
      LocalFlow flow = flowData.getFlow();
      try {
        flow.bfs(new DAG.Operator<FlowElementNode>() {
          public void process(FlowElementNode elemNode) {
            FlowElement flowElem = elemNode.getFlowElement();
            if (!flowElem.isClosed()) {
              try {
                flowElem.close();
              } catch (IOException ioe) {
                LOG.error("IOException when closing flow element: " + ioe);
              } catch (InterruptedException ie) {
                LOG.error("InterruptedException when closing flow element: " + ie);
              }
            }

            // All FlowElements that we see will have LocalContext subclass contexts.
            // Get the output queue from this, and remove it from the tracking set.
            LocalContext elemContext = (LocalContext) flowElem.getContext();
            mAllFlowQueues.remove(elemContext.getPendingEventQueue());
          }
        });
      } catch (DAGOperatorException doe) {
        // Shouldn't get here with this operator.
        LOG.error("Unexpected dag op exn: " + doe);
      }

      // Notify external threads that this flow is complete.
      flowData.cancel();
    }

    private void cancelFlow(FlowId id) {
      LOG.info("Closing flow: " + id);
      ActiveFlowData flowData = mActiveFlows.get(id);
      if (null == flowData) {
        LOG.error("Cannot cancel flow: No flow available for id: " + id);
        return;
      }
      cancelFlowInner(flowData);
      mActiveFlows.remove(id);
    }

    /** @return true if 'id' refers to an active flow. */
    private boolean isActive(FlowId id) {
      return mActiveFlows.get(id) != null;
    }

    private void cancelAllFlows() {
      LOG.info("Closing all flows");
      Set<Map.Entry<FlowId, ActiveFlowData>> flowSet = mActiveFlows.entrySet();
      Iterator<Map.Entry<FlowId, ActiveFlowData>> flowIter = flowSet.iterator();
      while (flowIter.hasNext()) {
        Map.Entry<FlowId, ActiveFlowData> entry = flowIter.next();
        cancelFlowInner(entry.getValue());
      }

      mActiveFlows.clear();
    }

    /**
     * Populate the provided map with info about all running flows. This map
     * is provided by the user process, so we need to synchronize on it before
     * writing. Furthermore, we must notify the calling thread when it is ready
     * for consumption.
     */
    private void listFlows(Map<FlowId, FlowInfo> outMap) {
      assert outMap != null;
      synchronized (outMap) {
        for (Map.Entry<FlowId, ActiveFlowData> entry : mActiveFlows.entrySet()) {
          FlowId id = entry.getKey();
          ActiveFlowData activeData = entry.getValue();
          outMap.put(id, new FlowInfo(id, activeData.getFlow().getQuery()));
        }

        // Notify the calling thread when we're done.
        outMap.notify();
      }
    }

    @Override
    public void run() {
      try {
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
              try {
                deployFlow(newFlow);
              } catch (Exception e) {
                LOG.error("Exception deploying flow: " + e);
              }
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
            case ElementComplete:
              // Remove a specific FlowElement from service; it's done.
              LocalCompletionEvent completionEvent = (LocalCompletionEvent) nextOp.getDatum();
              FlowElement downstream = null;
              try {
                LocalContext context = completionEvent.getContext();
                mAllFlowQueues.remove(context.getPendingEventQueue());

                if (context instanceof DirectCoupledFlowElemContext) {
                  // Notify the downstream FlowElement that it too should close.
                  DirectCoupledFlowElemContext directContext =
                      (DirectCoupledFlowElemContext) context;
                  downstream = directContext.getDownstream();
                  // TODO(aaron): Is this call correctly placed? Or even relevant?
                  downstream.completeWindow();
                  downstream.closeUpstream();
                } else if (context instanceof SinkFlowElemContext) {
                  // We have received close() notification from the last element in a flow.
                  // Remove the entire flow from service.
                  // TODO(aaron): Are multiple SinkFlowElemContexts possible per flow?
                  // If so, we need to wait for the last of these...
                  SinkFlowElemContext sinkContext = (SinkFlowElemContext) context;
                  FlowId id = sinkContext.getFlowId();
                  LOG.info("Processing complete for flow: " + id);
                  if (isActive(id)) {
                    // If the flow is closing naturally, cancel it. If it's
                    // already canceled (inactive), don't do this twice.
                    cancelFlow(id);
                  }
                }
              } catch (IOException ioe) {
                LOG.error("IOException closing flow element (" + downstream + "): " + ioe);
              } catch (InterruptedException ie) {
                LOG.error("Interruption closing downstream element: " + ie);
              }
              break;
            case Join:
              FlowJoinRequest joinReq = (FlowJoinRequest) nextOp.getDatum();
              FlowId id = joinReq.getFlowId();
              Ref<Boolean> waitObj = joinReq.getJoinObj();
              ActiveFlowData flowData = mActiveFlows.get(id);
              if (null == flowData) {
                // This flow id is already canceled. Return immediately.
                synchronized (waitObj) {
                  waitObj.item = Boolean.TRUE;
                  waitObj.notify();
                }
              } else {
                // Mark the waitObj as one we should notify when the flow is canceled.
                flowData.subscribeToCancelation(waitObj);
              }
              break;
            case ListFlows:
              Map<FlowId, FlowInfo> resultMap = (Map<FlowId, FlowInfo>) nextOp.getDatum();
              listFlows(resultMap);
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
              if (LOG.isDebugEnabled() && queue.size() > 0) {
                LOG.debug("Dequeuing from queue: " + queue + " with size=" + queue.size());
              }
              while (!queue.isEmpty()) {
                PendingEvent pe = queue.remove();
                FlowElement fe = pe.getFlowElement();
                EventWrapper e = pe.getEvent();
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
      } finally {
        // Shut down the embedded Flume instance before we exit the thread.
        if (mFlumeStarted) {
          mFlumeConfig.stop();
        }
      }
    }
  }

  /** The configuration for this environment instance. */
  private Configuration mConf;

  /** Next flow id to assign to new flows. */
  private long mNextFlowId;

  /** The AST generator used to parse user queries. */ 
  private ASTGenerator mGenerator;

  /**
   * Mapping from named outputs to the memory elements fulfilling those outputs.
   * If clients will be accessing elements of this map, then it needs to be
   * internally synchronized.
   */
  private Map<String, MemoryOutputElement> mMemoryOutputMap;

  /** 
   * Manager for the embedded Flume instances in this environment.
   * References to this object are distributed in the client thread,
   * but its methods are used only in the execution thread.
   */
  private EmbeddedFlumeConfig mFlumeConfig;

  /** The thread that does the actual flow execution. */
  private LocalEnvThread mLocalThread;

  /** set to true after connect(). */
  private boolean mConnected;

  /**
   * Queue of control events passed from the console thread to the worker thread
   * (e.g., "deploy stream", "cancel stream", etc.)
   */
  private BlockingQueue<ControlOp> mControlQueue;

  /** Max len for mControlQueue. */
  private static final int MAX_QUEUE_LEN = 100;

  /**
   * The root symbol table where streams, etc are defined. Used in the
   * user thread for AST and plan walking.
   */
  private SymbolTable mRootSymbolTable; 

  /**
   * Main constructor.
   */
  public LocalEnvironment(Configuration conf) {
    this(conf, new HashSymbolTable(new BuiltInSymbolTable()),
        new HashMap<String, MemoryOutputElement>());
  }

  /**
   * Constructor for testing; allows dependency injection.
   */
  public LocalEnvironment(Configuration conf, SymbolTable rootSymbolTable,
      Map<String, MemoryOutputElement> memoryOutputMap) {
    mConf = conf;
    mRootSymbolTable = rootSymbolTable;
    mMemoryOutputMap = memoryOutputMap;

    mGenerator = new ASTGenerator();
    mNextFlowId = 0;
    mControlQueue = new ArrayBlockingQueue<ControlOp>(MAX_QUEUE_LEN);
    mFlumeConfig = new EmbeddedFlumeConfig(mConf);
    mLocalThread = this.new LocalEnvThread();
  }

  @Override
  public void connect() throws IOException {
    mLocalThread.start();
    mConnected = true;
  }

  @Override
  public boolean isConnected() {
    return mConnected;
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

      stmt.accept(new AssignFieldLabelsVisitor());
      stmt.accept(new TypeChecker(mRootSymbolTable));
      stmt.accept(new JoinKeyVisitor()); // Must be after TC.
      stmt.accept(new JoinNameVisitor());
      PlanContext planContext = new PlanContext();
      planContext.setConf(mConf);
      planContext.setSymbolTable(mRootSymbolTable);
      PlanContext retContext = stmt.createExecPlan(planContext);
      msgBuilder.append(retContext.getMsgBuilder().toString());
      FlowSpecification spec = retContext.getFlowSpec();
      if (null != spec) {
        spec.setQuery(query);
        // Given a flow specification from the AST, run it through
        // necessary post-processing and optimization phases.
        spec.bfs(new PropagateSchemas());
        if (retContext.isExplain()) {
          // We just should explain this flow, but not actually add it.
          msgBuilder.append("Execution plan:\n");
          msgBuilder.append(spec.toString());
          msgBuilder.append("\n");
        } else {
          flowId = addFlow(spec);
        }
      }
    } catch (VisitException ve) {
      msgBuilder.append("Error processing command: " + ve.getMessage());
    } catch (RecognitionException re) {
      msgBuilder.append("Error parsing command: " + re.getMessage());
    } catch (DAGOperatorException doe) {
      msgBuilder.append("Error processing plan: " + doe.getMessage());
    }

    return new QuerySubmitResponse(msgBuilder.toString(), flowId);
  }

  @Override
  public FlowId addFlow(FlowSpecification spec) throws InterruptedException {
    if (null != spec) {
      // Turn the specification into a physical plan and run it.
      FlowId flowId = new FlowId(mNextFlowId++);
      LocalFlowBuilder flowBuilder = new LocalFlowBuilder(flowId, mRootSymbolTable,
          mFlumeConfig, mMemoryOutputMap);
      try {
        spec.reverseBfs(flowBuilder);
      } catch (DAGOperatorException doe) {
        // An exception occurred when creating the physical plan.
        // LocalFlowBuilder put a message for the user in here; print it
        // without a stack trace. The flow cannot be executed.
        LOG.error(doe.getMessage());
        return null;
      }
      LocalFlow localFlow = flowBuilder.getLocalFlow();
      localFlow.setQuery(spec.getQuery());
      if (localFlow.getRootSet().size() == 0) {
        // No nodes created (empty flow, or DDL-only flow, etc.)
        return null;
      } else {
        mControlQueue.put(new ControlOp(ControlOp.Code.AddFlow, localFlow));
        return flowId;
      }
    } else {
      return null;
    }
  }

  @Override
  public void cancelFlow(FlowId id) throws InterruptedException, IOException {
    mControlQueue.put(new ControlOp(ControlOp.Code.CancelFlow, id));
  }

  @Override
  public void joinFlow(FlowId id) throws InterruptedException {
    Ref<Boolean> joinObj = new Ref<Boolean>();
    synchronized (joinObj) {
      mControlQueue.put(new ControlOp(ControlOp.Code.Join, new FlowJoinRequest(id, joinObj)));
      joinObj.wait();
    }
  }

  @Override
  public boolean joinFlow(FlowId id, long timeout) throws InterruptedException {
    Ref<Boolean> joinObj = new Ref<Boolean>();
    joinObj.item = Boolean.FALSE;
    synchronized (joinObj) {
      mControlQueue.put(new ControlOp(ControlOp.Code.Join, new FlowJoinRequest(id, joinObj)));
      joinObj.wait(timeout);
      return joinObj.item;
    }
  }

  @Override
  public Map<FlowId, FlowInfo> listFlows() throws InterruptedException {
    Map<FlowId, FlowInfo> outData = new TreeMap<FlowId, FlowInfo>();
    synchronized (outData) {
      while (true) {
        mControlQueue.put(new ControlOp(ControlOp.Code.ListFlows, outData));
        try {
          outData.wait();
          break;
        } catch (InterruptedException ie) {
        }
      }
    }
    return outData;
  }

  /**
   * Stop the local environment and shut down any flows operating therein.
   */
  @Override
  public void disconnect() throws InterruptedException {
    mControlQueue.put(new ControlOp(ControlOp.Code.CancelAll, null));
    mControlQueue.put(new ControlOp(ControlOp.Code.ShutdownThread, null));
    mLocalThread.join();
    mConnected = false;
  }
}

