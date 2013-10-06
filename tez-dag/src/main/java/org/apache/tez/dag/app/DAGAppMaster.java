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

package org.apache.tez.dag.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Task;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEvent;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventDAGFinished;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGEvent;
import org.apache.tez.dag.app.dag.event.DAGEventType;
import org.apache.tez.dag.app.dag.event.TaskAttemptEvent;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.dag.event.TaskEvent;
import org.apache.tez.dag.app.dag.event.TaskEventType;
import org.apache.tez.dag.app.dag.event.VertexEvent;
import org.apache.tez.dag.app.dag.event.VertexEventType;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.launcher.ContainerLauncher;
import org.apache.tez.dag.app.launcher.ContainerLauncherImpl;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeEventType;
import org.apache.tez.dag.app.rm.node.AMNodeMap;
import org.apache.tez.dag.app.taskclean.TaskCleaner;
import org.apache.tez.dag.app.taskclean.TaskCleanerImpl;
import org.apache.tez.dag.history.DAGHistoryEvent;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.history.avro.HistoryEventType;
import org.apache.tez.dag.history.events.AMStartedEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.runtime.library.common.security.JobTokenSecretManager;

/**
 * The Map-Reduce Application Master.
 * The state machine is encapsulated in the implementation of Job interface.
 * All state changes happens via Job interface. Each event
 * results in a Finite State Transition in Job.
 *
 * MR AppMaster is the composition of loosely coupled services. The services
 * interact with each other via events. The components resembles the
 * Actors model. The component acts on received event and send out the
 * events to other components.
 * This keeps it highly concurrent with no or minimal synchronization needs.
 *
 * The events are dispatched by a central Dispatch mechanism. All components
 * register to the Dispatcher.
 *
 * The information is shared across different components using AppContext.
 */

@SuppressWarnings("rawtypes")
public class DAGAppMaster extends AbstractService {

  private static final Log LOG = LogFactory.getLog(DAGAppMaster.class);

  /**
   * Priority of the DAGAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private Clock clock;
  private final boolean isSession;
  private long appsStartTime;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  private AMContainerMap containers;
  private AMNodeMap nodes;
  private AppContext context;
  private Configuration amConf;
  private Dispatcher dispatcher;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  private ContainerHeartbeatHandler containerHeartbeatHandler;
  private TaskHeartbeatHandler taskHeartbeatHandler;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager =
      new JobTokenSecretManager();
  private DagEventDispatcher dagEventDispatcher;
  private VertexEventDispatcher vertexEventDispatcher;
  private TaskSchedulerEventHandler taskSchedulerEventHandler;
  private HistoryEventHandler historyEventHandler;

  private DAGAppMasterShutdownHandler shutdownHandler =
      new DAGAppMasterShutdownHandler();

  private DAGAppMasterState state;

  DAGClientServer clientRpcServer;
  private DAGClientHandler clientHandler;

  private DAG currentDAG;
  private Credentials fsTokens = new Credentials(); // Filled during init
  private UserGroupInformation currentUser; // Will be setup during init

  private AtomicBoolean sessionStopped = new AtomicBoolean(false);
  private long sessionTimeoutInterval;
  private long lastDAGCompletionTime;
  private Timer dagSubmissionTimer;

  // DAG Counter
  private final AtomicInteger dagCounter = new AtomicInteger();

  // Session counters
  private final AtomicInteger submittedDAGs = new AtomicInteger();
  private final AtomicInteger successfulDAGs = new AtomicInteger();
  private final AtomicInteger failedDAGs = new AtomicInteger();
  private final AtomicInteger killedDAGs = new AtomicInteger();

  // Service Handler
  private DAGServiceHandler serviceHandler;

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      long appSubmitTime, boolean isSession) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
        new SystemClock(), appSubmitTime, isSession);
  }

  public DAGAppMaster(ApplicationAttemptId applicationAttemptId,
      ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
      Clock clock, long appSubmitTime, boolean isSession) {
    super(DAGAppMaster.class.getName());
    this.clock = clock;
    this.startTime = clock.getTime();
    this.appSubmitTime = appSubmitTime;
    this.appAttemptID = applicationAttemptId;
    this.containerID = containerId;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.state = DAGAppMasterState.NEW;
    this.isSession = isSession;
    // TODO Metrics
    //this.metrics = DAGAppMetrics.create();
    LOG.info("Created DAGAppMaster for application " + applicationAttemptId);
  }

  @Override
  public void serviceInit(final Configuration conf) throws Exception {

    this.state = DAGAppMasterState.INITED;

    this.amConf = conf;
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    downloadTokensAndSetupUGI(conf);

    context = new RunningAppContext(conf);

    clientHandler = new DAGClientHandler();

    dispatcher = createDispatcher();

    serviceHandler = new DAGServiceHandler(dispatcher);

    serviceHandler.addIfService(dispatcher, false);

    clientRpcServer = new DAGClientServer(clientHandler);
    serviceHandler.addIfService(clientRpcServer, true);

    taskHeartbeatHandler = createTaskHeartbeatHandler(context, conf);
    serviceHandler.addIfService(taskHeartbeatHandler, true);

    containerHeartbeatHandler = createContainerHeartbeatHandler(context, conf);
    serviceHandler.addIfService(containerHeartbeatHandler, true);

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context,
        taskHeartbeatHandler, containerHeartbeatHandler);
    serviceHandler.addIfService(taskAttemptListener, true);

    containers = new AMContainerMap(containerHeartbeatHandler,
        taskAttemptListener, context);
    serviceHandler.addIfService(containers, true);
    dispatcher.register(AMContainerEventType.class, containers);

    nodes = new AMNodeMap(dispatcher.getEventHandler(), context);
    serviceHandler.addIfService(nodes, true);
    dispatcher.register(AMNodeEventType.class, nodes);

    //service to do the task cleanup
    taskCleaner = createTaskCleaner(context);
    serviceHandler.addIfService(taskCleaner, true);

    this.dagEventDispatcher = new DagEventDispatcher();
    this.vertexEventDispatcher = new VertexEventDispatcher();

    //register the event dispatchers
    dispatcher.register(DAGAppMasterEventType.class, new DAGAppMasterEventHandler());
    dispatcher.register(DAGEventType.class, dagEventDispatcher);
    dispatcher.register(VertexEventType.class, vertexEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class,
        new TaskAttemptEventDispatcher());
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);

    taskSchedulerEventHandler = new TaskSchedulerEventHandler(context,
        clientRpcServer, dispatcher.getEventHandler());
    serviceHandler.addIfService(taskSchedulerEventHandler, true);
    dispatcher.register(AMSchedulerEventType.class,
        taskSchedulerEventHandler);
    serviceHandler.addIfServiceDependency(taskSchedulerEventHandler,
        clientRpcServer);

    containerLauncher = createContainerLauncher(context);
    serviceHandler.addIfService(containerLauncher, true);
    dispatcher.register(NMCommunicatorEventType.class, containerLauncher);

    historyEventHandler = new HistoryEventHandler(context);
    serviceHandler.addIfService(historyEventHandler, true);
    dispatcher.register(HistoryEventType.class, historyEventHandler);

    this.sessionTimeoutInterval = 1000 * amConf.getInt(
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
            TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT);

    serviceHandler.initServices(conf);
    super.serviceInit(conf);
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  /**
   * Exit call. Just in a function call to enable testing.
   */
  protected void sysexit() {
    System.exit(0);
  }

  private synchronized void handle(DAGAppMasterEvent event) {
    switch (event.getType()) {
    case INTERNAL_ERROR:
      state = DAGAppMasterState.ERROR;
      if(currentDAG != null) {
        // notify dag to finish which will send the DAG_FINISHED event
        LOG.info("Internal Error. Notifying dags to finish.");
        sendEvent(new DAGEvent(currentDAG.getID(), DAGEventType.INTERNAL_ERROR));
      } else {
        LOG.info("Internal Error. Finishing directly as no dag is active.");
        shutdownHandler.shutdown();
      }
      break;
    case DAG_FINISHED:
      DAGAppMasterEventDAGFinished finishEvt =
          (DAGAppMasterEventDAGFinished) event;
      if (!isSession) {
        setStateOnDAGCompletion();
        LOG.info("Shutting down on completion of dag:" +
              finishEvt.getDAGId().toString());
        shutdownHandler.shutdown();
      } else {
        LOG.info("DAG completed, dagId="
            + finishEvt.getDAGId().toString()
            + ", dagState=" + finishEvt.getDAGState());
        lastDAGCompletionTime = clock.getTime();
        switch(finishEvt.getDAGState()) {
        case SUCCEEDED:
          successfulDAGs.incrementAndGet();
          break;
        case ERROR:
        case FAILED:
          failedDAGs.incrementAndGet();
          break;
        case KILLED:
          killedDAGs.incrementAndGet();
          break;
        default:
          LOG.fatal("Received a DAG Finished Event with state="
              + finishEvt.getDAGState()
              + ". Error. Shutting down.");
          state = DAGAppMasterState.ERROR;
          shutdownHandler.shutdown();
          break;
        }
        if (!state.equals(DAGAppMasterState.ERROR)) {
          if (!sessionStopped.get()) {
            LOG.info("Waiting for next DAG to be submitted.");
            state = DAGAppMasterState.IDLE;
          } else {
            LOG.info("Session shutting down now.");
            state = DAGAppMasterState.SUCCEEDED;
            shutdownHandler.shutdown();
          }
        }
      }
      break;
    default:
      throw new TezUncheckedException(
          "AppMaster: No handler for event type: " + event.getType());
    }
  }

  private class DAGAppMasterEventHandler implements
      EventHandler<DAGAppMasterEvent> {
    @Override
    public void handle(DAGAppMasterEvent event) {
      DAGAppMaster.this.handle(event);
    }
  }

  private class DAGAppMasterShutdownHandler {
    private AtomicBoolean shutdownHandled = new AtomicBoolean(false);

    public void shutdown() {
      if(!shutdownHandled.compareAndSet(false, true)) {
        LOG.info("Ignoring multiple shutdown events");
        return;
      }

      LOG.info("Handling DAGAppMaster shutdown");

      AMShutdownRunnable r = new AMShutdownRunnable();
      Thread t = new Thread(r, "AMShutdownThread");
      t.start();
    }

    private class AMShutdownRunnable implements Runnable {
      @Override
      public void run() {
        // TODO:currently just wait for some time so clients can know the
        // final states. Will be removed once RM come on. TEZ-160.
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        try {
          // Stop all services
          // This will also send the final report to the ResourceManager
          LOG.info("Calling stop for all the services");
          stop();

          //Bring the process down by force.
          //Not needed after HADOOP-7140
          LOG.info("Exiting DAGAppMaster..GoodBye!");
          sysexit();

        } catch (Throwable t) {
          LOG.warn("Graceful stop failed ", t);
        }
      }
    }
  }

  /** Create and initialize (but don't start) a single dag. */
  protected DAG createDAG(DAGPlan dagPB) {
    TezDAGID dagId = new TezDAGID(appAttemptID.getApplicationId(),
        dagCounter.incrementAndGet());

    Iterator<PlanKeyValuePair> iter =
        dagPB.getDagKeyValues().getConfKeyValuesList().iterator();
    Configuration dagConf = new Configuration(amConf);

    while (iter.hasNext()) {
      PlanKeyValuePair keyValPair = iter.next();
      dagConf.set(keyValPair.getKey(), keyValPair.getValue());
    }

    // create single dag
    DAG newDag =
        new DAGImpl(dagId, dagConf, dagPB, dispatcher.getEventHandler(),
            taskAttemptListener, jobTokenSecretManager, fsTokens, clock,
            currentUser.getShortUserName(),
            taskHeartbeatHandler, context);

    return newDag;
  } // end createDag()


  /**
   * Obtain the tokens needed by the job and put them in the UGI
   * @param conf
   */
  protected void downloadTokensAndSetupUGI(Configuration conf) {
    // TODO remove - TEZ-71
    try {
      this.currentUser = UserGroupInformation.getCurrentUser();

      if (UserGroupInformation.isSecurityEnabled()) {
        // Read the file-system tokens from the localized tokens-file.
        Path jobSubmitDir =
            FileContext.getLocalFSFileContext().makeQualified(
                new Path(new File(TezConfiguration.JOB_SUBMIT_DIR)
                    .getAbsolutePath()));
        Path jobTokenFile =
            new Path(jobSubmitDir, TezConfiguration.APPLICATION_TOKENS_FILE);
        fsTokens.addAll(Credentials.readTokenStorageFile(jobTokenFile, conf));
        LOG.info("jobSubmitDir=" + jobSubmitDir + " jobTokenFile="
            + jobTokenFile);

        for (Token<? extends TokenIdentifier> tk : fsTokens.getAllTokens()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Token of kind " + tk.getKind()
                + "in current ugi in the AppMaster for service "
                + tk.getService());
          }
          currentUser.addToken(tk); // For use by AppMaster itself.
        }
      }
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
  }

  protected TaskAttemptListener createTaskAttemptListener(AppContext context,
      TaskHeartbeatHandler thh, ContainerHeartbeatHandler chh) {
    TaskAttemptListener lis =
        new TaskAttemptListenerImpTezDag(context, thh, chh,jobTokenSecretManager);
    return lis;
  }

  protected TaskHeartbeatHandler createTaskHeartbeatHandler(AppContext context,
      Configuration conf) {
    TaskHeartbeatHandler thh = new TaskHeartbeatHandler(context, conf.getInt(
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT,
        TezConfiguration.TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT));
    return thh;
  }

  protected ContainerHeartbeatHandler createContainerHeartbeatHandler(
      AppContext context, Configuration conf) {
    ContainerHeartbeatHandler chh = new ContainerHeartbeatHandler(context,
        conf.getInt(
            TezConfiguration.TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT,
            TezConfiguration.TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT_DEFAULT));
    return chh;
  }


  protected TaskCleaner createTaskCleaner(AppContext context) {
    return new TaskCleanerImpl(context);
  }

  protected ContainerLauncher
      createContainerLauncher(final AppContext context) {
    return new ContainerLauncherImpl(context);
  }

  public ApplicationId getAppID() {
    return appAttemptID.getApplicationId();
  }

  public ApplicationAttemptId getAttemptID() {
    return appAttemptID;
  }

  public int getStartCount() {
    return appAttemptID.getAttemptId();
  }

  public AppContext getContext() {
    return context;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public ContainerLauncher getContainerLauncher() {
    return containerLauncher;
  }

  public TaskAttemptListener getTaskAttemptListener() {
    return taskAttemptListener;
  }

  public ContainerId getAppContainerId() {
    return containerID;
  }

  public String getAppNMHost() {
    return nmHost;
  }

  public int getAppNMPort() {
    return nmPort;
  }

  public int getAppNMHttpPort() {
    return nmHttpPort;
  }

  public DAGAppMasterState getState() {
    return state;
  }

  public List<String> getDiagnostics() {
    if (!isSession) {
      if(currentDAG != null) {
        return currentDAG.getDiagnostics();
      }
    } else {
      return Collections.singletonList("Session stats:"
          + "submittedDAGs=" + submittedDAGs.get()
          + ", successfulDAGs=" + successfulDAGs.get()
          + ", failedDAGs=" + failedDAGs.get());
    }
    return null;
  }

  public float getProgress() {
    if(currentDAG != null && currentDAG.getState() == DAGState.RUNNING) {
      return currentDAG.getProgress();
    }
    return 0;
  }

  private synchronized void setStateOnDAGCompletion() {
    DAGAppMasterState oldState = state;
    if(isSession) {
      return;
    }
    switch(currentDAG.getState()) {
    case SUCCEEDED:
      state = DAGAppMasterState.SUCCEEDED;
      break;
    case FAILED:
      state = DAGAppMasterState.FAILED;
      break;
    case KILLED:
      state = DAGAppMasterState.KILLED;
      break;
    case ERROR:
      state = DAGAppMasterState.ERROR;
      break;
    default:
      state = DAGAppMasterState.ERROR;
      break;
    }
    LOG.info("On DAG completion. Old state: "
        + oldState + " new state: " + state);
  }

  synchronized void shutdownTezAM() {
    sessionStopped.set(true);
    if (currentDAG != null
        && !currentDAG.isComplete()) {
      //send a DAG_KILL message
      LOG.info("Sending a kill event to the current DAG"
          + ", dagId=" + currentDAG.getID());
      sendEvent(new DAGEvent(currentDAG.getID(), DAGEventType.DAG_KILL));
    } else {
      LOG.info("No current running DAG, shutting down the AM");
      if (isSession && !state.equals(DAGAppMasterState.ERROR)) {
        state = DAGAppMasterState.SUCCEEDED;
      }
      shutdownHandler.shutdown();
    }
  }

  synchronized String submitDAGToAppMaster(DAGPlan dagPlan)
      throws TezException  {
    if(currentDAG != null
        && !state.equals(DAGAppMasterState.IDLE)) {
      throw new TezException("App master already running a DAG");
    }
    if (state.equals(DAGAppMasterState.ERROR)
        || sessionStopped.get()) {
      throw new TezException("AM unable to accept new DAG submissions."
          + " In the process of shutting down");
    }

    // RPC server runs in the context of the job user as it was started in
    // the job user's UGI context
    LOG.info("Starting DAG submitted via RPC");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing DAG plan to: "
          + TezConfiguration.TEZ_PB_PLAN_TEXT_NAME);

      File outFile = new File(TezConfiguration.TEZ_PB_PLAN_TEXT_NAME);
      try {
        PrintWriter printWriter = new PrintWriter(outFile);
        String dagPbString = dagPlan.toString();
        printWriter.println(dagPbString);
        printWriter.close();
      } catch (IOException e) {
        throw new TezException("Failed to write TEZ_PLAN to "
            + outFile.toString(), e);
      }
    }

    submittedDAGs.incrementAndGet();
    startDAG(dagPlan);
    return currentDAG.getID().toString();
  }

  public class DAGClientHandler {

    public List<String> getAllDAGs() throws TezException {
      return Collections.singletonList(currentDAG.getID().toString());
    }

    public DAGStatus getDAGStatus(String dagIdStr) throws TezException {
      return getDAG(dagIdStr).getDAGStatus();
    }

    public VertexStatus getVertexStatus(String dagIdStr, String vertexName)
        throws TezException{
      VertexStatus status = getDAG(dagIdStr).getVertexStatus(vertexName);
      if(status == null) {
        throw new TezException("Unknown vertexName: " + vertexName);
      }

      return status;
    }

    DAG getDAG(String dagIdStr) throws TezException {
      TezDAGID dagId = TezDAGID.fromString(dagIdStr);
      if(dagId == null) {
        throw new TezException("Bad dagId: " + dagIdStr);
      }

      if(currentDAG == null) {
        throw new TezException("No running dag at present");
      }
      if(!dagId.equals(currentDAG.getID())) {
        throw new TezException("Unknown dagId: " + dagIdStr);
      }

      return currentDAG;
    }

    public void tryKillDAG(String dagIdStr)
        throws TezException {
      DAG dag = getDAG(dagIdStr);
      LOG.info("Sending client kill to dag: " + dagIdStr);
      //send a DAG_KILL message
      sendEvent(new DAGEvent(dag.getID(), DAGEventType.DAG_KILL));
    }

    public synchronized String submitDAG(DAGPlan dagPlan) throws TezException {
      return submitDAGToAppMaster(dagPlan);
    }

    public synchronized void shutdownAM() {
      LOG.info("Received message to shutdown AM");
      shutdownTezAM();
    }

    public synchronized TezSessionStatus getSessionStatus() throws TezException {
      if (!isSession) {
        throw new TezException("Unsupported operation as AM not running in"
            + " session mode");
      }
      switch (state) {
      case NEW:
      case INITED:
        return TezSessionStatus.INITIALIZING;
      case IDLE:
        return TezSessionStatus.READY;
      case RUNNING:
        return TezSessionStatus.RUNNING;
      case ERROR:
      case FAILED:
      case SUCCEEDED:
      case KILLED:
        return TezSessionStatus.SHUTDOWN;
      }
      return TezSessionStatus.INITIALIZING;
    }
  }

  private class RunningAppContext implements AppContext {

    private DAG dag;
    private final Configuration conf;
    private final ClusterInfo clusterInfo = new ClusterInfo();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    public RunningAppContext(Configuration config) {
      this.conf = config;
    }

    @Override
    public DAGAppMaster getAppMaster() {
      return DAGAppMaster.this;
    }

    @Override
    public Configuration getAMConf() {
      return conf;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return appAttemptID;
    }

    @Override
    public ApplicationId getApplicationID() {
      return appAttemptID.getApplicationId();
    }

    @Override
    public String getApplicationName() {
      return appName;
    }

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public DAG getCurrentDAG() {
      try {
        rLock.lock();
        return dag;
      } finally {
        rLock.unlock();
      }
    }

    @Override
    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    @Override
    public String getUser() {
      return dag.getUserName();
    }

    @Override
    public Clock getClock() {
      return clock;
    }

    @Override
    public ClusterInfo getClusterInfo() {
      return this.clusterInfo;
    }

    @Override
    public AMContainerMap getAllContainers() {
      return containers;
    }

    @Override
    public AMNodeMap getAllNodes() {
      return nodes;
    }

    @Override
    public TaskSchedulerEventHandler getTaskScheduler() {
      return taskSchedulerEventHandler;
    }

    @Override
    public Map<ApplicationAccessType, String> getApplicationACLs() {
      if (getServiceState() != STATE.STARTED) {
        throw new TezUncheckedException(
            "Cannot get ApplicationACLs before all services have started");
      }
      return taskSchedulerEventHandler.getApplicationAcls();
    }

    @Override
    public TezDAGID getCurrentDAGID() {
      try {
        rLock.lock();
        if(dag != null) {
          return dag.getID();
        }
        return null;
      } finally {
        rLock.unlock();
      }
    }

    @Override
    public void setDAG(DAG dag) {
      try {
        wLock.lock();
        this.dag = dag;
      } finally {
        wLock.unlock();
      }
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void serviceStart() throws Exception {

    //start all the components
    serviceHandler.startServices();
    super.serviceStart();

    this.state = DAGAppMasterState.IDLE;

    // metrics system init is really init & start.
    // It's more test friendly to put it here.
    DefaultMetricsSystem.initialize("DAGAppMaster");

    this.appsStartTime = clock.getTime();
    AMStartedEvent startEvent = new AMStartedEvent(appAttemptID,
        startTime, appsStartTime, appSubmitTime);
    dispatcher.getEventHandler().handle(
        new DAGHistoryEvent(startEvent));

    this.lastDAGCompletionTime = clock.getTime();

    if (!isSession) {
      startDAG();
    } else {
      LOG.info("In Session mode. Waiting for DAG over RPC");
      this.dagSubmissionTimer = new Timer(true);
      this.dagSubmissionTimer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          checkAndHandleSessionTimeout();
        }
      }, sessionTimeoutInterval, sessionTimeoutInterval/10);
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (isSession) {
      sessionStopped.set(true);
    }
    if (this.dagSubmissionTimer != null) {
      this.dagSubmissionTimer.cancel();
    }
    serviceHandler.stopServices();
    super.serviceStop();
  }

  private class DagEventDispatcher implements EventHandler<DAGEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(DAGEvent event) {
      ((EventHandler<DAGEvent>)context.getCurrentDAG()).handle(event);
    }
  }

  private class TaskEventDispatcher implements EventHandler<TaskEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskEvent event) {
      Task task =
          context.getCurrentDAG().getVertex(event.getTaskID().getVertexID()).
              getTask(event.getTaskID());
      ((EventHandler<TaskEvent>)task).handle(event);
    }
  }

  private class TaskAttemptEventDispatcher
          implements EventHandler<TaskAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(TaskAttemptEvent event) {
      DAG dag = context.getCurrentDAG();
      Task task =
          dag.getVertex(event.getTaskAttemptID().getTaskID().getVertexID()).
              getTask(event.getTaskAttemptID().getTaskID());
      TaskAttempt attempt = task.getAttempt(event.getTaskAttemptID());
      ((EventHandler<TaskAttemptEvent>) attempt).handle(event);
    }
  }

  private class VertexEventDispatcher
    implements EventHandler<VertexEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void handle(VertexEvent event) {
      DAG dag = context.getCurrentDAG();
      org.apache.tez.dag.app.dag.Vertex vertex =
          dag.getVertex(event.getVertexId());
      ((EventHandler<VertexEvent>) vertex).handle(event);
    }
  }

  private static void validateInputParam(String value, String param)
      throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  private synchronized void checkAndHandleSessionTimeout() {
    if (this.state.equals(DAGAppMasterState.RUNNING)
        || sessionStopped.get()) {
      // DAG running or session already completed, cannot timeout session
      return;
    }
    long currentTime = clock.getTime();
    if (currentTime < (lastDAGCompletionTime + sessionTimeoutInterval)) {
      return;
    }
    LOG.info("Session timed out"
        + ", lastDAGCompletionTime=" + lastDAGCompletionTime + " ms"
        + ", sessionTimeoutInterval=" + sessionTimeoutInterval + " ms");
    shutdownTezAM();
  }

  public static void main(String[] args) {
    try {
      Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      String containerIdStr =
          System.getenv(Environment.CONTAINER_ID.name());
      String nodeHostString = System.getenv(Environment.NM_HOST.name());
      String nodePortString = System.getenv(Environment.NM_PORT.name());
      String nodeHttpPortString =
          System.getenv(Environment.NM_HTTP_PORT.name());
      String appSubmitTimeStr =
          System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);

      validateInputParam(appSubmitTimeStr,
          ApplicationConstants.APP_SUBMIT_TIME_ENV);

      ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
      ApplicationAttemptId applicationAttemptId =
          containerId.getApplicationAttemptId();

      long appSubmitTime = Long.parseLong(appSubmitTimeStr);

      Configuration conf = new Configuration(new YarnConfiguration());
      TezUtils.addUserSpecifiedTezConfiguration(conf);

      String jobUserName = System
          .getenv(ApplicationConstants.Environment.USER.name());

      // Do not automatically close FileSystem objects so that in case of
      // SIGTERM I have a chance to write out the job history. I'll be closing
      // the objects myself.
      conf.setBoolean("fs.automatic.close", false);

      // Command line options
      Options opts = new Options();
      opts.addOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION,
          false, "Run Tez Application Master in Session mode");

      CommandLine cliParser = new GnuParser().parse(opts, args);

      DAGAppMaster appMaster =
          new DAGAppMaster(applicationAttemptId, containerId, nodeHostString,
              Integer.parseInt(nodePortString),
              Integer.parseInt(nodeHttpPortString), appSubmitTime,
              cliParser.hasOption(TezConstants.TEZ_SESSION_MODE_CLI_OPTION));
      ShutdownHookManager.get().addShutdownHook(
        new DAGAppMasterShutdownHook(appMaster), SHUTDOWN_HOOK_PRIORITY);

      initAndStartAppMaster(appMaster, conf,
          jobUserName);

    } catch (Throwable t) {
      LOG.fatal("Error starting DAGAppMaster", t);
      System.exit(1);
    }
  }

  // The shutdown hook that runs when a signal is received AND during normal
  // close of the JVM.
  static class DAGAppMasterShutdownHook implements Runnable {
    DAGAppMaster appMaster;
    DAGAppMasterShutdownHook(DAGAppMaster appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      if(appMaster.getServiceState() == STATE.STOPPED) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("DAGAppMaster already stopped. Ignoring signal");
        }
        return;
      }

      if(appMaster.getServiceState() == STATE.STARTED) {
        // Notify TaskScheduler that a SIGTERM has been received so that it
        // unregisters quickly with proper status
        LOG.info("DAGAppMaster received a signal. Signaling TaskScheduler");
        appMaster.taskSchedulerEventHandler.setSignalled(true);
      }

      if (EnumSet.of(DAGAppMasterState.NEW, DAGAppMasterState.INITED,
          DAGAppMasterState.IDLE, DAGAppMasterState.RUNNING)
          .contains(appMaster.state)) {
            // DAG not in a final state. Must have receive a KILL signal
        appMaster.state = DAGAppMasterState.KILLED;
      }
      appMaster.stop();
    }
  }

  private void startDAG() throws IOException {
    FileInputStream dagPBBinaryStream = null;
    try {
      DAGPlan dagPlan = null;

      // Read the protobuf DAG
      DAGPlan.Builder dagPlanBuilder = DAGPlan.newBuilder();
      dagPBBinaryStream = new FileInputStream(
          TezConfiguration.TEZ_PB_PLAN_BINARY_NAME);
      dagPlanBuilder.mergeFrom(dagPBBinaryStream);

      dagPlan = dagPlanBuilder.build();

      startDAG(dagPlan);

    } finally {
      if (dagPBBinaryStream != null) {
        dagPBBinaryStream.close();
      }
    }
  }

  private void startDAG(DAGPlan dagPlan) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running a DAG with " + dagPlan.getVertexCount()
          + " vertices ");
      for (VertexPlan v : dagPlan.getVertexList()) {
        LOG.debug("DAG has vertex " + v.getName());
      }
    }

    // Job name is the same as the app name until we support multiple dags
    // for an app later
    appName = dagPlan.getName();

    // /////////////////// Create the job itself.
    DAG newDAG = createDAG(dagPlan);
    startDAG(newDAG);
  }

  private void startDAG(DAG dag) {
    currentDAG = dag;
    this.state = DAGAppMasterState.RUNNING;

    // End of creating the job.
    ((RunningAppContext) context).setDAG(currentDAG);

    // create a job event for job initialization
    DAGEvent initDagEvent = new DAGEvent(currentDAG.getID(), DAGEventType.DAG_INIT);
    // Send init to the job (this does NOT trigger job execution)
    // This is a synchronous call, not an event through dispatcher. We want
    // job-init to be done completely here.
    dagEventDispatcher.handle(initDagEvent);

    // All components have started, start the job.
    /** create a job-start event to get this ball rolling */
    DAGEvent startDagEvent = new DAGEvent(currentDAG.getID(), DAGEventType.DAG_START);
    /** send the job-start event. this triggers the job execution. */
    sendEvent(startDagEvent);
  }

  // TODO XXX Does this really need to be a YarnConfiguration ?
  protected static void initAndStartAppMaster(final DAGAppMaster appMaster,
      final Configuration conf, String jobUserName) throws IOException,
      InterruptedException {
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation appMasterUgi = UserGroupInformation
        .createRemoteUser(jobUserName);
    appMasterUgi.addCredentials(credentials);
    appMasterUgi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        appMaster.init(conf);
        appMaster.start();
        return null;
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void sendEvent(Event<?> event) {
    dispatcher.getEventHandler().handle(event);
  }
}
