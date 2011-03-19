// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.antlr.runtime.RecognitionException;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Pair;

import com.odiago.flumebase.client.ClientConsoleImpl;

import com.odiago.flumebase.exec.BuiltInSymbolTable;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.ExecEnvironment;
import com.odiago.flumebase.exec.FlowElement;
import com.odiago.flumebase.exec.FlowId;
import com.odiago.flumebase.exec.FlowInfo;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.OutputElement;
import com.odiago.flumebase.exec.QuerySubmitResponse;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.flume.EmbeddedFlumeConfig;

import com.odiago.flumebase.lang.AssignFieldLabelsVisitor;
import com.odiago.flumebase.lang.CountStarVisitor;
import com.odiago.flumebase.lang.IdentifyAggregates;
import com.odiago.flumebase.lang.JoinKeyVisitor;
import com.odiago.flumebase.lang.JoinNameVisitor;
import com.odiago.flumebase.lang.ReplaceWindows;
import com.odiago.flumebase.lang.TypeChecker;
import com.odiago.flumebase.lang.VisitException;

import com.odiago.flumebase.parser.ASTGenerator;
import com.odiago.flumebase.parser.SQLStatement;

import com.odiago.flumebase.plan.FlowSpecification;
import com.odiago.flumebase.plan.PlanContext;
import com.odiago.flumebase.plan.PropagateSchemas;

import com.odiago.flumebase.server.SessionId;
import com.odiago.flumebase.server.UserSession;

import com.odiago.flumebase.util.DAG;
import com.odiago.flumebase.util.DAGOperatorException;
import com.odiago.flumebase.util.Ref;
import com.odiago.flumebase.util.StringUtils;

import com.odiago.flumebase.util.concurrent.ArrayBoundedSelectableQueue;
import com.odiago.flumebase.util.concurrent.Select;
import com.odiago.flumebase.util.concurrent.Selectable;
import com.odiago.flumebase.util.concurrent.SelectableQueue;
import com.odiago.flumebase.util.concurrent.SyncSelectableQueue;

/**
 * Standalone local execution environment for flows.
 */
public class LocalEnvironment extends ExecEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(
      LocalEnvironment.class.getName());

  /** Config key specifying whether we automatically watch a flow when we create it or not. */
  public static final String AUTO_WATCH_FLOW_KEY = "flumebase.flow.autowatch";
  public static final boolean DEFAULT_AUTO_WATCH_FLOW = true;

  /** Config key specifying the session id of the submitting user for a query. */
  public static final String SUBMITTER_SESSION_ID_KEY = "flumebase.query.submitter.session.id";

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
      WatchFlow,       // Subscribe to a flow's output.
      UnwatchFlow,     // Unsubscribe from a flow's output.
      GetWatchList,    // Get a list of flows being watched by a session.
      SetFlowName,     // Set the name of the output stream for a flow.
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
   * A request to watch/unwatch a flow.
   */
  private static class WatchRequest {
    public final boolean mIsWatch; // true for watch, false for unwatch.
    public final SessionId mSessionId;
    public final FlowId mFlowId;

    public WatchRequest(SessionId sessionId, FlowId flowId, boolean isWatch) {
      mIsWatch = isWatch;
      mSessionId = sessionId;
      mFlowId = flowId;
    }
  }

  /**
   * The thread where the active flows in the local environment actually operate.
   */
  private class LocalEnvThread extends Thread {

    /** The set of running flows. */
    private Map<FlowId, ActiveFlowData> mActiveFlows;

    /** Mapping from an input queue to the FlowElement it is feeding values to. */
    private Map<SelectableQueue<Object>, FlowElement> mInputQueues;

    /** The selector that lets us read from multiple producers */
    private Select<Object> mSelect;

    /** Unbounded queue used in-thread to post completion events back to the main loop. */
    private SelectableQueue<Object> mCompletionEventQueue;

    /**
     * Set of queues which should be watched for emptiness; when they transition
     * to empty, notify the associated downstream element of the upstream element's
     * closure.
     */
    private Set<SelectableQueue<Object>> mCloseQueues;

    public LocalEnvThread() {
      mActiveFlows = new HashMap<FlowId, ActiveFlowData>();
      mSelect = new Select<Object>();
      mCompletionEventQueue = new SyncSelectableQueue<Object>();
      mInputQueues = new HashMap<SelectableQueue<Object>, FlowElement>();
      mCloseQueues = new HashSet<SelectableQueue<Object>>();

      setName("LocalEnvWorker");
    }

    private void deployFlow(LocalFlow newFlow) throws IOException, InterruptedException {
      final ActiveFlowData activeFlowData = new ActiveFlowData(newFlow);

      Configuration flowConf = newFlow.getConf();
      if (flowConf.getBoolean(AUTO_WATCH_FLOW_KEY, DEFAULT_AUTO_WATCH_FLOW)) {
        // Subscribe to this flow before running it, so we guarantee the user
        // sees all the results.
        long idNum = flowConf.getLong(SUBMITTER_SESSION_ID_KEY, -1);
        SessionId submitterSessionId = new SessionId(idNum);
        UserSession session = getSession(submitterSessionId);
        if (session != null) {
          activeFlowData.addSession(session);
        } else {
          LOG.warn("Invalid session id number: " + idNum);
        }
      }

      // If we haven't yet started Flume, and the flow requires Flume-based sources,
      // start Flume.
      if (newFlow.requiresFlume() && !mFlumeConfig.isRunning()) {
        mFlumeConfig.start();
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
            elemContext.initControlQueue(mCompletionEventQueue);
            elemContext.setFlowData(activeFlowData);

            elemContext.createDownstreamQueues();
            List<SelectableQueue<Object>> elemBuffers = elemContext.getDownstreamQueues();
            if (null != elemBuffers) {
              List<FlowElement> downstreams = elemContext.getDownstream();
              // Bind each queue to its downstream element.
              for (int i = 0; i < elemBuffers.size(); i++) {
                SelectableQueue<Object> elemBuffer = elemBuffers.get(i);
                if (null != elemBuffer) {
                  FlowElement downstream = downstreams.get(i);
                  mInputQueues.put(elemBuffer, downstream);
                  mSelect.add(elemBuffer); // And watch this queue for updates.
                }
              }
            }

            try {
              LOG.debug("Opening flow element of class: " + flowElem.getClass().getName());
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

      mActiveFlows.put(newFlow.getId(), activeFlowData);
    }

    private void cancelFlowInner(ActiveFlowData flowData) {
      // Close all FlowElements in the flow, and remove their output queues
      // from the set of queues we track.
      LocalFlow flow = flowData.getFlow();
      try {
        flow.rankTraversal(new DAG.Operator<FlowElementNode>() {
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
            List<SelectableQueue<Object>> outQueues = elemContext.getDownstreamQueues();
            if (null != outQueues) {
              for (SelectableQueue<Object> outQueue : outQueues) {
                if (null != outQueue) {
                  mSelect.remove(outQueue);
                  mInputQueues.remove(outQueue);
                  mCloseQueues.remove(outQueue);
                }
              }
            }
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
      if (mActiveFlows.size() == 0) {
        return;
      }

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
          outMap.put(id, new FlowInfo(id, activeData.getFlow().getQuery(),
              activeData.getStreamName()));
        }

        // Notify the calling thread when we're done.
        outMap.notify();
      }
    }

    /**
     * Sign up the specified session to watch a given flow.
     */
    private void watch(WatchRequest watchReq) {
      UserSession userSession = getSession(watchReq.mSessionId);
      if (null == userSession) {
        LOG.warn("Cannot watch flow from user session " + watchReq.mSessionId
            + "; no such session");
        return;
      }

      ActiveFlowData flow = mActiveFlows.get(watchReq.mFlowId);
      if (null == flow) {
        LOG.warn("Cannot watch flow from user session " + watchReq.mSessionId
            + "; no such flow");
        return;
      }

      if(watchReq.mIsWatch) {
        flow.addSession(userSession);
      } else {
        flow.removeSession(userSession);
      }
    }

    /**
     * Populate the specified flowList with a list of FlowIds that are
     * being watched by the specified sessionId.
     * The flowList is provided by a client in another thread; synchronize
     * on it and notify the other thread when the request is complete.
     */
    private void getWatchList(SessionId sessionId, List<FlowId> flowList) {
      synchronized(flowList) {
        UserSession session = getSession(sessionId);
        if (null != session) {
          for (ActiveFlowData activeFlow : mActiveFlows.values()) {
            List<UserSession> subscribers = activeFlow.getSubscribers();
            if (subscribers.contains(session)) {
              flowList.add(activeFlow.getFlowId());
            }
          }
        } else {
          LOG.error("GetWatchList for sessionId " + sessionId + ": no such session");
        }

        flowList.notify(); // We're done. Wake up the client.
      }
    }

    /**
     * The specified queue is empty and its upstream element is closed. Notify
     * the downstream element of this closure, and remove the queue from the
     * set of things we track.
     */
    private void closeQueue(SelectableQueue<Object> queue, FlowElement flowElem)
        throws IOException, InterruptedException {
      flowElem.closeUpstream();
      mSelect.remove(queue);
      mInputQueues.remove(queue);
      mCloseQueues.remove(queue);
    }

    /**
     * Update the OutputElement of a flow to use a different output stream
     * name for the output.
     */
    private void setFlowName(final FlowId flowId, final String name) {
      ActiveFlowData flowData = mActiveFlows.get(flowId);
      if (null == flowData) {
        LOG.error("Cannot set flow name for flow id " + flowId + ": no such flow.");
        return;
      }

      try {
        // NOTE - This assumes a single OutputElement per flow; we find it by
        // reverseBfs because we assume it's at the end. If there are multiple
        // OutputElements in the flow, we'll get them all trying to open the
        // same node...

        flowData.getFlow().reverseBfs(new DAG.Operator<FlowElementNode>() {
          public void process(FlowElementNode node) throws DAGOperatorException {
            FlowElement flowElem = node.getFlowElement();
            if (flowElem instanceof OutputElement) {
              try {
                ((OutputElement) flowElem).setFlumeTarget(name);
              } catch (IOException ioe) {
                throw new DAGOperatorException(ioe);
              }
            }
          }
        });
      } catch (DAGOperatorException doe) {
        LOG.error("Error setting output stream name: " + doe);
      }
    }

    @Override
    public void run() {
      mSelect.add(mControlQueue); // Listen to events on the control queue.
      mSelect.add(mCompletionEventQueue);
      try {
        boolean isFinished = false;

        while (true) {
          Selectable<Object> nextQueue = null;
          try {
            nextQueue = mSelect.join();
          } catch (InterruptedException ie) {
            // This can happen to notify us we're done processing, etc.
          }

          if (null == nextQueue) {
            continue;
          }

          Object nextAction = null;
          synchronized (nextQueue) {
            if (nextQueue.canRead()) {
              try {
                nextAction = nextQueue.read();
              } catch (InterruptedException ie) {
                // This can happen if we're closing shop fast. We'll just loop around.
              }
            }
          }

          if (null == nextAction) {
            continue;
          } else if (nextAction instanceof ControlOp) {
            ControlOp nextOp = (ControlOp) nextAction;

            switch (nextOp.getOpCode()) {
            case AddFlow:
              LocalFlow newFlow = (LocalFlow) nextOp.getDatum();
              try {
                deployFlow(newFlow);
              } catch (Exception e) {
                LOG.error("Exception deploying flow: " + StringUtils.stringifyException(e));
              } finally {
                // Client waited on this object to know when deployment is done.
                synchronized (newFlow) {
                  newFlow.setDeployed(true);
                  newFlow.notify();
                }
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
            case WatchFlow:
              WatchRequest watchReq = (WatchRequest) nextOp.getDatum();
              watch(watchReq);
              break;
            case UnwatchFlow:
              WatchRequest unwatchReq = (WatchRequest) nextOp.getDatum();
              watch(unwatchReq); // the request has the isWatch flag set false. 
              break;
            case GetWatchList:
              Pair<SessionId, List<FlowId>> getReq =
                  (Pair<SessionId, List<FlowId>>) nextOp.getDatum();
              getWatchList(getReq.getLeft(), getReq.getRight());
              break;
            case SetFlowName:
              Pair<FlowId, String> flowNameData =
                  (Pair<FlowId, String>) nextOp.getDatum();
              setFlowName(flowNameData.getLeft(), flowNameData.getRight());
            case Noop:
              // Don't do any control operation; skip ahead to event processing.
              break;
            case ElementComplete:
              // Remove a specific FlowElement from service; it's done.
              LocalCompletionEvent completionEvent = (LocalCompletionEvent) nextOp.getDatum();
              try {
                LocalContext context = completionEvent.getContext();

                List<SelectableQueue<Object>> downstreamQueues =
                    context.getDownstreamQueues();
                List<FlowElement> downstreamElements = context.getDownstream();
                if (null == downstreamElements || downstreamElements.size() == 0) {
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
                } else if (null == downstreamQueues || downstreamQueues.size() == 0) {
                  // Has elements, but no queues. Notify the downstream
                  // FlowElement(s) to close too.
                  for (FlowElement downstream : downstreamElements) {
                    downstream.closeUpstream();
                  }
                } else {
                  // May have downstream queues. For each downstream element, close it
                  // immediately if it has no queue, or an empty queue. Otherwise,
                  // watch these queues for emptiness.
                  assert downstreamQueues.size() == downstreamElements.size();
                  for (int i = 0; i < downstreamElements.size(); i++) {
                    SelectableQueue<Object> downstreamQueue = downstreamQueues.get(i);
                    FlowElement downstreamElement = downstreamElements.get(i);
                    if (downstreamQueue == null) {
                      // Close directly.
                      downstreamElement.closeUpstream();
                    } else if (downstreamQueue.size() == 0) {
                      // Queue's dry, close it down.
                      closeQueue(downstreamQueue, downstreamElement);
                    } else {
                      // Watch this queue for completion.
                      mCloseQueues.add(downstreamQueue);
                    }

                  }
                }
              } catch (IOException ioe) {
                LOG.error("IOException closing flow element: " + ioe);
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

            if (isFinished) {
              // Stop immediately; ignore any further event processing or control work.
              break;
            }
          } else if (nextAction instanceof EventWrapper) {
            // Process this event with its associated FlowElement.
            // Look up the correct FlowElement based on the queue->FE map.
            FlowElement processor = mInputQueues.get(nextQueue);
            if (null == processor) {
              LOG.error("No FlowElement for input queue " + nextQueue);
            } else {
              try {
                processor.takeEvent((EventWrapper) nextAction);
              } catch (IOException ioe) {
                // TODO(aaron): Encountering an exception mid-flow should cancel the flow.
                LOG.error("Flow element encountered IOException: " + ioe);
              } catch (InterruptedException ie) {
                LOG.error("Flow element encountered InterruptedException: " + ie);
              }
            }

            if (((SelectableQueue<Object>) nextQueue).size() == 0
                && mCloseQueues.contains(nextQueue)) {
              // We just transitioned this FE's input queue to empty, and it was closed
              // upstream. Notify the downstream element of this closure.
              try {
                closeQueue((SelectableQueue<Object>) nextQueue, processor);
              } catch (IOException ioe) {
                LOG.error("IOException closing flow element: " + ioe);
              } catch (InterruptedException ie) {
                LOG.error("InterruptedException closing flow element: " + ie);
              }
            }
          } else {
            LOG.error("Do not know what to do with queue element " + nextAction
                + " of class " + nextAction.getClass().getName());
          }
        }
      } finally {
        // Shut down the embedded Flume instance before we exit the thread.
        if (mFlumeConfig.isRunning()) {
          mFlumeConfig.stop();
        }
      }
    }
  }

  /** A UserSession describing the console of this process. */
  private static final UserSession LOCAL_SESSION;

  static {
    LOCAL_SESSION = new UserSession(new SessionId(0), null, new ClientConsoleImpl());
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
  private SelectableQueue<Object> mControlQueue; // Actually full of ControlOp instances

  /** Max len for mControlQueue, or any FlowElement's input queue. */
  static final int MAX_QUEUE_LEN = 100;

  /**
   * The root symbol table where streams, etc are defined. Used in the
   * user thread for AST and plan walking.
   */
  private SymbolTable mRootSymbolTable; 

  /**
   * Main constructor.
   */
  public LocalEnvironment(Configuration conf) {
    this(conf,
        new HashSymbolTable(new BuiltInSymbolTable()),
        new HashMap<String, MemoryOutputElement>(),
        new EmbeddedFlumeConfig(conf));
  }

  /**
   * Constructor for testing; allows dependency injection.
   */
  public LocalEnvironment(Configuration conf,
      SymbolTable rootSymbolTable,
      Map<String, MemoryOutputElement> memoryOutputMap,
      EmbeddedFlumeConfig flumeConfig) {
    mConf = conf;
    mRootSymbolTable = rootSymbolTable;
    mMemoryOutputMap = memoryOutputMap;

    mGenerator = new ASTGenerator();
    mNextFlowId = 0;
    mControlQueue = new ArrayBoundedSelectableQueue<Object>(MAX_QUEUE_LEN);
    mFlumeConfig = flumeConfig;
    mLocalThread = this.new LocalEnvThread();
  }

  /** Given a Configuration that has SUBMITTER_SESSION_ID_KEY set, return the
   * UserSession corresponding to this SessionId. This is used to resolve the
   * submitter of a LocalFlow, FlowSpecification, etc.
   */
  private UserSession getSessionForConf(Configuration conf) {
    SessionId id = new SessionId(conf.getLong(SUBMITTER_SESSION_ID_KEY, -1));
    return getSession(id);
  }

  @Override
  public SessionId connect() throws IOException {
    mLocalThread.start();
    mConnected = true;
    return new SessionId(0); // Local user is always session 0.
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
  public QuerySubmitResponse submitQuery(String query, Map<String, String> options)
      throws InterruptedException {
    StringBuilder msgBuilder = new StringBuilder();
    FlowId flowId = null;

    // Build a configuration out of our conf and the user's options.
    Configuration planConf = new Configuration(mConf);
    for (Map.Entry<String, String> entry : options.entrySet()) {
      planConf.set(entry.getKey(), entry.getValue());
    }

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
      stmt.accept(new CountStarVisitor()); // Must be after assign labels, before TC.
      stmt.accept(new TypeChecker(mRootSymbolTable));
      stmt.accept(new ReplaceWindows()); // Must be after TC.
      stmt.accept(new JoinKeyVisitor()); // Must be after TC.
      stmt.accept(new JoinNameVisitor());
      stmt.accept(new IdentifyAggregates()); // Must be after TC.
      PlanContext planContext = new PlanContext();
      planContext.setConf(planConf);
      planContext.setSymbolTable(mRootSymbolTable);
      PlanContext retContext = stmt.createExecPlan(planContext);
      msgBuilder.append(retContext.getMsgBuilder().toString());
      FlowSpecification spec = retContext.getFlowSpec();
      if (null != spec) {
        spec.setQuery(query);
        spec.setConf(planConf);
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
      UserSession userSession = getSessionForConf(spec.getConf());
      LocalFlowBuilder flowBuilder = new LocalFlowBuilder(flowId, mRootSymbolTable,
          mFlumeConfig, mMemoryOutputMap, userSession);
      try {
        spec.reverseBfs(flowBuilder);
      } catch (DAGOperatorException doe) {
        // An exception occurred when creating the physical plan.
        // LocalFlowBuilder put a message for the user in here; print it
        // without a stack trace. The flow cannot be executed.
        userSession.sendErr(doe.getMessage());
        return null;
      }
      LocalFlow localFlow = flowBuilder.getLocalFlow();
      localFlow.setQuery(spec.getQuery());
      localFlow.setConf(spec.getConf());
      if (localFlow.getRootSet().size() == 0) {
        // No nodes created (empty flow, or DDL-only flow, etc.)
        return null;
      } else {
        synchronized (localFlow) {
          mControlQueue.put(new ControlOp(ControlOp.Code.AddFlow, localFlow));
          while (!localFlow.isDeployed()) {
            localFlow.wait();
          }
        }
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
  public void watchFlow(SessionId sessionId, FlowId flowId) throws InterruptedException {
    mControlQueue.put(new ControlOp(ControlOp.Code.WatchFlow,
        new WatchRequest(sessionId, flowId, true)));
  }

  @Override
  public void unwatchFlow(SessionId sessionId, FlowId flowId) throws InterruptedException {
    mControlQueue.put(new ControlOp(ControlOp.Code.UnwatchFlow,
        new WatchRequest(sessionId, flowId, false)));
  }

  @Override
  public Map<FlowId, FlowInfo> listFlows() throws InterruptedException {
    Map<FlowId, FlowInfo> outData = new TreeMap<FlowId, FlowInfo>();
    synchronized (outData) {
      mControlQueue.put(new ControlOp(ControlOp.Code.ListFlows, outData));
      outData.wait();
    }
    return outData;
  }

  @Override
  public List<FlowId> listWatchedFlows(SessionId sessionId) throws InterruptedException {
    List<FlowId> outList = new ArrayList<FlowId>();
    Pair<SessionId, List<FlowId>> args = new Pair<SessionId, List<FlowId>>(sessionId, outList);
    synchronized (outList) {
      mControlQueue.put(new ControlOp(ControlOp.Code.GetWatchList, args));
      outList.wait();
    }
    return outList;
  }

  @Override
  public void setFlowName(FlowId flowId, String name) throws InterruptedException {
    Pair<FlowId, String> nameReq = new Pair<FlowId, String>(flowId, name);
    mControlQueue.put(new ControlOp(ControlOp.Code.SetFlowName, nameReq));
  }

  /**
   * Stop the local environment and shut down any flows operating therein.
   */
  @Override
  public void disconnect(SessionId sessionId) throws InterruptedException {
    shutdown();
  }

  @Override
  public void shutdown() throws InterruptedException {
    mControlQueue.put(new ControlOp(ControlOp.Code.CancelAll, null));
    mControlQueue.put(new ControlOp(ControlOp.Code.ShutdownThread, null));
    mLocalThread.join();
    mConnected = false;
  }

  /**
   * For the LocalEnvironment, there is only the local user session.
   */
  @Override
  protected UserSession getSession(SessionId id) {
    return LOCAL_SESSION;
  }
}

