// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.exec.FlowId;

import com.odiago.rtengine.server.UserSession;

import com.odiago.rtengine.util.CloseHandler;
import com.odiago.rtengine.util.Ref;

/**
 * Container for information maintained by the local environment
 * regarding an active flow.
 */
public class ActiveFlowData implements CloseHandler<UserSession> {
  /** The flow itself. */
  private final LocalFlow mLocalFlow;

  /**
   * Set of objects which will have notify() called when the flow is
   * complete. The join target is a reference to a Boolean, which
   * will be set to True when the flow is complete.
   */
  private final List<Ref<Boolean>> mJoinTargets;

  private final List<UserSession> mWatchingSessions;

  public ActiveFlowData(LocalFlow flow) {
    mLocalFlow = flow;
    mJoinTargets = new ArrayList<Ref<Boolean>>();
    mWatchingSessions = new ArrayList<UserSession>();
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

  /** This session now watches the output of the flow. */
  public void addSession(UserSession session) {
    if (!mWatchingSessions.contains(session)) {
      mWatchingSessions.add(session);
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
