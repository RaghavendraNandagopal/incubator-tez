/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class AMSchedulerEventTALaunchRequest extends AMSchedulerEvent {

  // TODO Get rid of remoteTask from here. Can be forgottent after it has been assigned.
  //.... Maybe have the Container talk to the TaskAttempt to pull in the remote task.

  private final TezTaskAttemptID attemptId;
  private final String[] hosts;
  private final String[] racks;
  private final Priority priority;
  private final Resource capability;
  private final ContainerContext containerContext;

  private final TaskSpec remoteTaskSpec;
  private final TaskAttempt taskAttempt;

  public AMSchedulerEventTALaunchRequest(TezTaskAttemptID attemptId,
      Resource capability,
      TaskSpec remoteTaskSpec, TaskAttempt ta,
      String[] hosts, String[] racks, Priority priority, ContainerContext containerContext) {
    super(AMSchedulerEventType.S_TA_LAUNCH_REQUEST);
    this.attemptId = attemptId;
    this.capability = capability;
    this.remoteTaskSpec = remoteTaskSpec;
    this.taskAttempt = ta;
    this.hosts = hosts;
    this.racks = racks;
    this.priority = priority;
    this.containerContext = containerContext;
  }

  public TezTaskAttemptID getAttemptID() {
    return this.attemptId;
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }

  public String[] getRacks() {
    return racks;
  }

  public Priority getPriority() {
    return priority;
  }

  public TaskSpec getRemoteTaskSpec() {
    return remoteTaskSpec;
  }

  public TaskAttempt getTaskAttempt() {
    return this.taskAttempt;
  }

  public ContainerContext getContainerContext() {
    return this.containerContext;
  }

  // Parameter replacement: @taskid@ will not be usable
  // ProfileTaskRange not available along with ContainerReUse

  /*Requirements to determine a container request.
   * + Data-local + Rack-local hosts.
   * + Resource capability
   * + Env - mapreduce.map.env / mapreduce.reduce.env can change. M/R log level.
   * - JobConf and JobJar file - same location.
   * - Distributed Cache - identical for map / reduce tasks at the moment.
   * - Credentials, tokens etc are identical.
   * + Command - dependent on map / reduce java.opts
   */
}
