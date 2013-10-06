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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.tez.dag.api.TezUncheckedException;

class DAGServiceHandler {

  static final Log LOG = LogFactory.getLog(DAGServiceHandler.class);


  private Map<Service, ServiceWithDependency> services = null;
  private Dispatcher dispatcher;

  DAGServiceHandler(Dispatcher dispatcher) {
    // must be LinkedHashMap to preserve order of service addition
    this.services = new LinkedHashMap<Service, ServiceWithDependency>();
    this.dispatcher = dispatcher;
  }

  protected void addIfService(Object object, boolean addDispatcher) {
    if (object instanceof Service) {
      Service service = (Service) object;
      ServiceWithDependency sd = new ServiceWithDependency(service);
      services.put(service, sd);
      if (addDispatcher) {
        addIfServiceDependency(service, dispatcher);
      }
    }
  }

  protected void addIfServiceDependency(Object object, Object dependency) {
    if (object instanceof Service && dependency instanceof Service) {
      Service service = (Service) object;
      Service dependencyService = (Service) dependency;
      ServiceWithDependency sd = services.get(service);
      sd.dependencies.add(dependencyService);
      dependencyService.registerServiceListener(sd);
    }
  }

  private class ServiceWithDependency implements ServiceStateChangeListener {
    ServiceWithDependency(Service service) {
      this.service = service;
    }

    Service service;
    List<Service> dependencies = new ArrayList<Service>();
    AtomicInteger dependenciesStarted = new AtomicInteger(0);
    volatile boolean canStart = false;
    volatile boolean dependenciesFailed = false;

    @Override
    public void stateChanged(Service dependency) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Service dependency: " + dependency.getName() + " notify"
            + " for service: " + service.getName());
      }
      if (dependency.isInState(Service.STATE.STARTED)) {
        if (dependenciesStarted.incrementAndGet() == dependencies.size()) {
          synchronized (this) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Service: " + service.getName() + " notified to start");
            }
            canStart = true;
            this.notifyAll();
          }
        }
      } else if (!service.isInState(Service.STATE.STARTED)
          && dependency.getFailureState() != null) {
        synchronized (this) {
          dependenciesFailed = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Service: " + service.getName() + " will fail to start"
                + " as dependent service " + dependency.getName()
                + " failed to start");
          }
          this.notifyAll();
        }
      }
    }

    void start() throws InterruptedException {
      if (dependencies.size() > 0) {
        synchronized (this) {
          while (!canStart) {
            this.wait(1000 * 60 * 3L);
            if (dependenciesFailed) {
              throw new TezUncheckedException("Skipping service start for "
                  + service.getName() + " as dependencies failed to start");
            }
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Service: " + service.getName() + " trying to start");
      }
      for (Service dependency : dependencies) {
        if (!dependency.isInState(Service.STATE.STARTED)) {
          LOG.info("Service: " + service.getName() + " not started because "
              + " service: " + dependency.getName() + " is in state: "
              + dependency.getServiceState());
          return;
        }
      }
      service.start();
    }
  }

  private class ServiceThread extends Thread {
    final ServiceWithDependency serviceWithDependency;
    Throwable error = null;

    public ServiceThread(ServiceWithDependency serviceWithDependency) {
      this.serviceWithDependency = serviceWithDependency;
      this.setName("ServiceThread:" + serviceWithDependency.service.getName());
    }

    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Starting thread " + serviceWithDependency.service.getName());
      }
      long start = System.currentTimeMillis();
      try {
        serviceWithDependency.start();
      } catch (Throwable t) {
        error = t;
      } finally {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Service: " + serviceWithDependency.service.getName()
              + " started in " + (System.currentTimeMillis() - start) + "ms");
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Service thread completed for "
            + serviceWithDependency.service.getName());
      }
    }
  }

  void startServices() {
    try {
      Throwable firstError = null;
      List<ServiceThread> threads = new ArrayList<ServiceThread>();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Begin parallel start");
      }
      for (ServiceWithDependency sd : services.values()) {
        // start the service. If this fails that service
        // will be stopped and an exception raised
        ServiceThread st = new ServiceThread(sd);
        threads.add(st);
      }

      for (ServiceThread st : threads) {
        st.start();
      }
      for (ServiceThread st : threads) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Waiting for service thread to join for " + st.getName());
        }
        st.join();
        if (st.error != null && firstError == null) {
          firstError = st.error;
        }
      }

      if (firstError != null) {
        throw ServiceStateException.convert(firstError);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("End parallel start");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  void initServices(Configuration conf) {
    for (ServiceWithDependency sd : services.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initing service : " + sd.service);
      }
      sd.service.init(conf);
    }
  }

  void stopServices() {
    // stop in reverse order of start
    List<Service> serviceList = new ArrayList<Service>(services.size());
    for (ServiceWithDependency sd : services.values()) {
      serviceList.add(sd.service);
    }
    Exception firstException = null;
    for (int i = services.size() - 1; i >= 0; i--) {
      Service service = serviceList.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping service : " + service);
      }
      Exception ex = ServiceOperations.stopQuietly(LOG, service);
      if (ex != null && firstException == null) {
        firstException = ex;
      }
    }
    // after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

}
