/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.bgd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.SVMLightParser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EventHandler;

/**
 *
 */
@DriverSide
@Unit
public class BGDDriver {
  private static final Logger LOG = Logger.getLogger(BGDDriver.class.getName());
  private static final String MASTER_TASK = "MasterTask";


  private final DataLoadingService dataLoadingService;
  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver communicationsGroup;
  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);
  private final AtomicInteger slaveIds = new AtomicInteger(0);
  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
  private final BGDControlParameters bgdControlParameters;
  private String communicationsGroupMasterContextId;


  @Inject
  public BGDDriver(final DataLoadingService dataLoadingService,
                   final GroupCommDriver groupCommDriver,
                   final BGDControlParameters bgdControlParameters) {
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.bgdControlParameters = bgdControlParameters;

    this.communicationsGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class,                               // NAME
        dataLoadingService.getNumberOfPartitions() + 1);           // Number of participants
    LOG.info("Obtained all communication group");


    communicationsGroup
        .addBroadcast(ControlMessageBroadcaster.class,      // NAME
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId(MASTER_TASK)                   // ID of the task that is the sender
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(ModelBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId(MASTER_TASK)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(LossAndGradientReducer.class,
            ReduceOperatorSpec
                .newBuilder()
                .setReceiverId(MASTER_TASK)                  // ID of the receiving task (R in the slides)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(LossAndGradientReduceFunction.class)
                .build())
        .finalise();

    LOG.log(Level.INFO, "Added operators to communicationsGroup");
  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {


    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.info("Got active context-" + activeContext.getId());

      if (!groupCommDriver.configured(activeContext)) {
        // The Context is not configured with the group communications service let's do that.
        submitGroupCommunicationsService(activeContext);
      } else {
        // The group communications service is already active on this context. We can submit the task.
        submitTask(activeContext);
      }
    }

    /**
     * @param activeContext a context to be configured with group communications.
     */
    private void submitGroupCommunicationsService(final ActiveContext activeContext) {
      final Configuration contextConfiguration = groupCommDriver.getContextConf();
      final String contextId = getContextID(contextConfiguration);
      if (!dataLoadingService.isDataLoadedContext(activeContext)) {
        communicationsGroupMasterContextId = contextId;
      }
      final Configuration serviceConfiguration = groupCommDriver.getServiceConf();
      activeContext.submitContextAndService(contextConfiguration, serviceConfiguration);
    }


    private void submitTask(final ActiveContext activeContext) {
      assert (groupCommDriver.configured(activeContext));

      final Configuration partialTaskConfiguration;
      if (activeContext.getId().equals(communicationsGroupMasterContextId) && !masterTaskSubmitted()) {
        partialTaskConfiguration = getMasterTaskConfiguration();
      } else {
        partialTaskConfiguration = getSlaveTaskConfiguration(getSlaveId(activeContext));
      }

      communicationsGroup.addTask(partialTaskConfiguration);
      final Configuration taskConfiguration = groupCommDriver.getTaskConfiguration(partialTaskConfiguration);
      activeContext.submitTask(taskConfiguration);
    }


    /**
     * @param activeContext
     * @return
     */
    private String getSlaveId(final ActiveContext activeContext) {
      return "SlaveTask-" + slaveIds.getAndIncrement();
    }

    /**
     * @return
     */
    private boolean masterTaskSubmitted() {
      return !masterSubmitted.compareAndSet(false, true);
    }
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask task) {
      LOG.info("Got complete task-" + task.getId());
      final byte[] returnValue = task.get();
      if (returnValue != null) {
        assert (task.getId().equals(MASTER_TASK));
        final List<Double> losses = BGDDriver.this.lossCodec.decode(returnValue);
        for (final Double loss : losses) {
          System.out.println(loss);
        }
      }
      task.getActiveContext().close();
    }

  }


  private Configuration getSlaveTaskConfiguration(final String taskId) {
    return Tang.Factory.getTang()
        .newConfigurationBuilder(
            TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, taskId)
                .set(TaskConfiguration.TASK, SlaveTask.class)
                .build())
        .bindNamedParameter(ModelDimensions.class, Integer.toString(bgdControlParameters.getDimensions()))
        .bindImplementation(Parser.class, SVMLightParser.class)
        .bindImplementation(LossFunction.class, bgdControlParameters.getLossFunction())
        .build();
  }


  private Configuration getMasterTaskConfiguration() {
    return Configurations.merge(
        TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, MASTER_TASK)
            .set(TaskConfiguration.TASK, MasterTask.class)
            .build(),
        bgdControlParameters.getConfiguration()
    );
  }

  /**
   * @param contextConf
   * @return
   */
  private static String getContextID(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }
}
