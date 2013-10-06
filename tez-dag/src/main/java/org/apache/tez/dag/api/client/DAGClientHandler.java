/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.dag.api.client;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.records.TezDAGID;

public class DAGClientHandler {
  static final Log LOG = LogFactory.getLog(DAGClientHandler.class);

  private DAGAppMaster dagAppMaster;

  public DAGClientHandler(DAGAppMaster dagAppMaster) {
    this.dagAppMaster = dagAppMaster;
  }

  public List<String> getAllDAGs() throws TezException {
    return Collections.singletonList(dagAppMaster.getContext().getCurrentDAG()
        .getID().toString());
  }

  public DAGStatus getDAGStatus(String dagIdStr) throws TezException {
    return getDAG(dagIdStr).getDAGStatus();
  }

  public VertexStatus getVertexStatus(String dagIdStr, String vertexName)
      throws TezException {
    VertexStatus status = getDAG(dagIdStr).getVertexStatus(vertexName);
    if (status == null) {
      throw new TezException("Unknown vertexName: " + vertexName);
    }

    return status;
  }

  DAG getDAG(String dagIdStr) throws TezException {
    TezDAGID dagId = TezDAGID.fromString(dagIdStr);
    if (dagId == null) {
      throw new TezException("Bad dagId: " + dagIdStr);
    }

    if (dagAppMaster.getContext().getCurrentDAG() == null) {
      throw new TezException("No running dag at present");
    }
    if (!dagId.equals(dagAppMaster.getContext().getCurrentDAG().getID())) {
      throw new TezException("Unknown dagId: " + dagIdStr);
    }

    return dagAppMaster.getContext().getCurrentDAG();
  }

  public void tryKillDAG(String dagIdStr) throws TezException {
    DAG dag = getDAG(dagIdStr);
    LOG.info("Sending client kill to dag: " + dagIdStr);
    // send a DAG_KILL message
    if (dagAppMaster == null) {
      throw new TezException("DAG App Master is not initialized");
    }
    dagAppMaster.sendEvent(new DAGEvent(dag.getID(), DAGEventType.DAG_KILL));
  }

  public synchronized String submitDAG(DAGPlan dagPlan) throws TezException {
    if (dagAppMaster == null) {
      throw new TezException("DAG App Master is not initialized");
    }
    return dagAppMaster.submitDAGToAppMaster(dagPlan);
  }

  public synchronized void shutdownAM() {
    LOG.info("Received message to shutdown AM");
    if (dagAppMaster != null) {
      dagAppMaster.shutdownTezAM();
    }
  }

  public synchronized TezSessionStatus getSessionStatus() throws TezException {
    if (dagAppMaster == null) {
      throw new TezException("DAG App Master is not initialized");
    }
    return dagAppMaster.getSessionStatus();
  }
}