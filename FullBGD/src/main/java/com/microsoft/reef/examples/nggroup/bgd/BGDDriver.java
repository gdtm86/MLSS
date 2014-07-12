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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.evaluator.context.parameters.ContextIdentifier;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.SVMLightParser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.DescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.MinEtaBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.EnableRampup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Eps;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.bgd.utils.SubConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationSerializer;
import com.microsoft.wake.EventHandler;

/**
 *
 */
@DriverSide
@Unit
public class BGDDriver {
  private static final Logger LOG = Logger.getLogger(BGDDriver.class.getName());

  private static final Tang TANG = Tang.Factory.getTang();

  private final DataLoadingService dataLoadingService;

  private final GroupCommDriver groupCommDriver;

  private final CommunicationGroupDriver allCommGroup;

  private final AtomicBoolean masterSubmitted = new AtomicBoolean(false);

  private final AtomicInteger slaveIds = new AtomicInteger(0);

  private String groupCommConfiguredMasterId;

  private final ConfigurationSerializer confSerializer;

  private final Map<String, RunningTask> runningTasks = new HashMap<>();

  private final AtomicBoolean jobComplete = new AtomicBoolean(false);

  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();

  private final BGDControlParameters bgdControlParameters;


  @Inject
  public BGDDriver(
      final DataLoadingService dataLoadingService,
      final GroupCommDriver groupCommDriver,
      final ConfigurationSerializer confSerializer,
      final BGDControlParameters bgdControlParameters) {
    this.dataLoadingService = dataLoadingService;
    this.groupCommDriver = groupCommDriver;
    this.confSerializer = confSerializer;
    this.bgdControlParameters = bgdControlParameters;

    final int minNumOfPartitions =
            bgdControlParameters.isRampup()
            ? bgdControlParameters.getMinParts()
            : dataLoadingService.getNumberOfPartitions();
    this.allCommGroup = this.groupCommDriver.newCommunicationGroup(
        AllCommunicationGroup.class,
        minNumOfPartitions + 1);
    LOG.info("Obtained all communication group");


    final ReduceFunction<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReduceFunction = new LossAndGradientReduceFunction();
    final ReduceFunction<Pair<Vector, Integer>> lineSearchReduceFunction = new LineSearchReduceFunction();
    allCommGroup
        .addBroadcast(ControlMessageBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(ModelBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(LossAndGradientReducer.class,
            ReduceOperatorSpec
                .newBuilder()
                .setReceiverId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(lossAndGradientReduceFunction.getClass())
                .build())
        .addBroadcast(ModelAndDescentDirectionBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(DescentDirectionBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(LineSearchEvaluationsReducer.class,
            ReduceOperatorSpec
                .newBuilder()
                .setReceiverId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(lineSearchReduceFunction.getClass())
                .build())
        .addBroadcast(MinEtaBroadcaster.class,
            BroadcastOperatorSpec
                .newBuilder()
                .setSenderId("MasterTask")
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .finalise();

    LOG.log(Level.INFO, "Added operators to allCommGroup");
  }

  final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask task) {
      LOG.info("Got complete task-" + task.getId());
      final byte[] retVal = task.get();
      if (retVal != null) {
        final List<Double> losses = BGDDriver.this.lossCodec.decode(retVal);
        for (final Double loss : losses) {
          System.out.println(loss);
        }
      }
      LOG.log(Level.FINEST, "Releasing All Contexts");
      synchronized (runningTasks) {
        LOG.info("Acquired lock on runningTasks. Removing " + task.getId());
        final RunningTask rTask = runningTasks.remove(task.getId());
        if (rTask != null) {
          LOG.info("Closing active context");
          task.getActiveContext().close();
        } else {
          LOG.info("Master must have closed my activ context");
        }
        if (task.getId().equals("MasterTask")) {
          jobComplete.set(true);
          LOG.info("I am Master. Job complete. Closing other running tasks: " + runningTasks.values());
          for (final RunningTask runTask : runningTasks.values()) {
            runTask.getActiveContext().close();
          }
          LOG.info("Clearing runningTasks");
          runningTasks.clear();
        }
      }
    }

  }

  final class TaskRunningHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      synchronized (runningTasks) {
        if (!jobComplete.get()) {
          LOG.info("Job has not completed yet. Adding to runningTasks");
          runningTasks.put(runningTask.getId(), runningTask);
        } else {
          LOG.info("Job has completed. Not adding to runningTasks. Closing the active context");
          runningTask.getActiveContext().close();
        }
      }
    }

  }

  final class ContextActiveHandler implements EventHandler<ActiveContext> {


    /**
     *
     */
    private static final double STARTUP_FAILURE_PROB = 0.01;

    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.info("Got active context-" + activeContext.getId());
      synchronized (runningTasks) {
        if (jobComplete.get()) {
          LOG.info("Job has completed. Not submitting any task. Closing activecontext");
          activeContext.close();
        }
      }
      /**
       * The active context can be either from
       * data loading service or after network
       * service has loaded contexts. So check
       * if the GroupCommDriver knows if it was
       * configured by one of the communication
       * groups
       */
      if (groupCommDriver.configured(activeContext)) {
        if (activeContext.getId().equals(groupCommConfiguredMasterId) && !masterTaskSubmitted()) {
          final Configuration partialTaskConf = Configurations.merge(
                  TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, "MasterTask")
                  .set(TaskConfiguration.TASK, MasterTask.class)
                  .build(),
                  SubConfiguration.from(
                          bgdControlParameters.getConfiguration(),
                          ModelDimensions.class, Lambda.class,
                          Eps.class, Iterations.class, EnableRampup.class));

          allCommGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.info("Submitting MasterTask conf");
          LOG.info(confSerializer.toString(taskConf));
          activeContext.submitTask(taskConf);
        } else {
          final Configuration partialTaskConf = Configurations.merge(getSlaveTaskConf(getSlaveId(activeContext)),
                  getTaskPoisonConfiguration());
          allCommGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          LOG.info("Submitting SlaveTask conf");
          LOG.info(confSerializer.toString(taskConf));
          activeContext.submitTask(taskConf);
        }
      } else {
        final Configuration contextConf = groupCommDriver.getContextConf();
        final String contextId = contextId(contextConf);
        if (!dataLoadingService.isDataLoadedContext(activeContext)) {
          groupCommConfiguredMasterId = contextId;
        }
        LOG.info("Submitting GCContext conf");
        LOG.info(confSerializer.toString(contextConf));

        final Configuration serviceConf = groupCommDriver.getServiceConf();
        LOG.info("Submitting Service conf");
        LOG.info(confSerializer.toString(serviceConf));
        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }

    private Configuration getTaskPoisonConfiguration() {
      return PoisonedConfiguration.TASK_CONF
          .set(PoisonedConfiguration.CRASH_PROBABILITY, STARTUP_FAILURE_PROB)
          .set(PoisonedConfiguration.CRASH_TIMEOUT, 1)
          .build();
    }

    /**
     * @param contextConf
     * @return
     */
    private String contextId(final Configuration contextConf) {
      try {
        final Injector injector = TANG.newInjector(contextConf);
        return injector.getNamedInstance(ContextIdentifier.class);
      } catch (final InjectionException e) {
        throw new RuntimeException("Unable to inject context identifier from context conf", e);
      }
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

  private Configuration getSlaveTaskConf(final String taskId) {
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

  final class TaskFailedHandler implements EventHandler<FailedTask> {

    @Override
    public void onNext(final FailedTask failedTask) {
      final String failedTaskId = failedTask.getId();
      LOG.info("Got failed Task " + failedTaskId);

      synchronized (runningTasks) {
        if (jobComplete.get()) {
          LOG.info("Job has completed. Not resubmitting. Closing activecontext");
          final RunningTask rTask = runningTasks.remove(failedTaskId);
          if (rTask != null) {
            rTask.getActiveContext().close();
          }
          return;
        } else {
          runningTasks.remove(failedTaskId);
        }
      }
      final ActiveContext activeContext = failedTask.getActiveContext().get();
      final Configuration partialTaskConf = getSlaveTaskConf(failedTaskId);
      //Do not add the task back
      //allCommGroup.addTask(partialTaskConf);
      final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
      LOG.info("Submitting SlaveTask conf");
      LOG.info(confSerializer.toString(taskConf));
      activeContext.submitTask(taskConf);
    }
  }

}
