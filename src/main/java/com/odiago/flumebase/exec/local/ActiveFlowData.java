// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.FlowId;

import com.odiago.flumebase.server.UserSession;

import com.odiago.flumebase.util.CloseHandler;
import com.odiago.flumebase.util.DAG;
import com.odiago.flumebase.util.DAGOperatorException;
import com.odiago.flumebase.util.Ref;

/**
 * Container for information maintained by the local environment
 * regarding an active flow.
 */
public class ActiveFlowData implements CloseHandler<UserSession> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ActiveFlowData.class.getName());

  /** The flow itself. */
  private final LocalFlow mLocalFlow;

  /**
   * Set of objects which will have notify() called when the flow is
   * complete. The join target is a reference to a Boolean, which
   * will be set to True when the flow is complete.
   */
  private final List<Ref<Boolean>> mJoinTargets;

  private final List<UserSession> mWatchingSessions;

  /** Stream name associated with the output of this flow. */
  private String mStreamName;

  public ActiveFlowData(LocalFlow flow) {
    mLocalFlow = flow;
    mJoinTargets = new ArrayList<Ref<Boolean>>();
    mWatchingSessions = new ArrayList<UserSession>();
    mStreamName = null;
  }

  public LocalFlow getFlow() {
    return mLocalFlow;
  }

  public FlowId getFlowId() {
    return mLocalFlow.getId();
  }

  public void setStreamName(String streamName) {
    mStreamName = streamName;
  }

  public String getStreamName() {
    return mStreamName;
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

  /** This session now watches the output of the flow. */
  public void addSession(final UserSession session) {
    if (!mWatchingSessions.contains(session)) {
      mWatchingSessions.add(session);
      // Perform onSubscribe actions for any FEs that need them.
      try {
        mLocalFlow.bfs(new DAG.Operator<FlowElementNode>() {
          public void process(FlowElementNode node) {
            node.getFlowElement().onConnect(session);
          }
        });
      } catch (DAGOperatorException doe) {
        // None to be thrown here..?
        LOG.error("Unexpected DAG operator exception: " + doe);
      }
      
      // Actually subscribe to the flow.
      session.subscribeToClose(this);
    }
  }

  /** This session no longer watches the output of the flow. */
  public void removeSession(UserSession session) {
    mWatchingSessions.remove(session);
    session.unsubscribeFromClose(this);
  }

  /** @return the list of UserSessions subscribing to this flow output. */
  public List<UserSession> getSubscribers() {
    return mWatchingSessions;
  }

  @Override
  public void handleClose(UserSession session) {
    // The specified session is notifying us that it has closed; we no longer watch its output.
    mWatchingSessions.remove(session);
  }
}
