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

import javax.inject.Inject;

import org.apache.hadoop.mapred.TextInputFormat;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.examples.nggroup.bgd.parameters.EvaluatorMemory;
import com.microsoft.reef.examples.nggroup.bgd.parameters.InputDir;
import com.microsoft.reef.examples.nggroup.bgd.parameters.NumSplits;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Timeout;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.io.network.nggroup.impl.GroupCommService;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Configurations;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.CommandLine;

/**
 * A client to submit BGD Jobs
 */
public class BGDClient {
  private final String input;
  private final int numSplits;
  private final int memory;

  private final BGDControlParameters bgdControlParameters;

  @Inject
  public BGDClient(final @Parameter(InputDir.class) String input,
                   final @Parameter(NumSplits.class) int numSplits,
                   final @Parameter(EvaluatorMemory.class) int memory,
                   final BGDControlParameters bgdControlParameters) {
    this.input = input;
    this.bgdControlParameters = bgdControlParameters;
    this.numSplits = numSplits;
    this.memory = memory;
  }

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @throws Exception
   */
  public void submit(final Configuration runtimeConfiguration, final String jobName) throws Exception {
    final Configuration driverConfiguration = getDriverConfiguration(jobName);
    Tang.Factory.getTang().newInjector(runtimeConfiguration).getInstance(REEF.class).submit(driverConfiguration);
  }

  /**
   * Runs BGD on the given runtime.
   *
   * @param runtimeConfiguration the runtime to run on.
   * @param jobName              the name of the job on the runtime.
   * @param timeout              the time after which the job will be killed if not completed, in ms
   * @return
   * @throws Exception
   */
  public LauncherStatus run(final Configuration runtimeConfiguration, final String jobName, final int timeout) throws Exception {
    final Configuration driverConfiguration = getDriverConfiguration(jobName);

    return DriverLauncher.getLauncher(runtimeConfiguration).run(driverConfiguration, timeout);
  }

  private final Configuration getDriverConfiguration(final String jobName) {
    return Configurations.merge(
        getDataLoadConfiguration(jobName),
        GroupCommService.getConfiguration(),
        bgdControlParameters.getConfiguration());
  }

  private Configuration getDataLoadConfiguration(final String jobName) {
    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(1)
        .setMemory(memory)
        .build();
    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(memory)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(input)
        .setNumberOfDesiredSplits(numSplits)
        .setComputeRequest(computeRequest)
        .setDriverConfigurationModule(EnvironmentUtils
            .addClasspath(DriverConfiguration.CONF, DriverConfiguration.GLOBAL_LIBRARIES)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, BGDDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, BGDDriver.TaskCompletedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, jobName))
        .build();
    return dataLoadConfiguration;
  }

  public static final BGDClient fromCommandLine(final String[] args) throws Exception {
    final JavaConfigurationBuilder configurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine commandLine = new CommandLine(configurationBuilder);
    commandLine.registerShortNameOfClass(InputDir.class);
    commandLine.registerShortNameOfClass(Timeout.class);
    commandLine.registerShortNameOfClass(EvaluatorMemory.class);
    commandLine.registerShortNameOfClass(NumSplits.class);
    BGDControlParameters.registerShortNames(commandLine);
    commandLine.processCommandLine(args);
    return Tang.Factory.getTang().newInjector(configurationBuilder.build()).getInstance(BGDClient.class);
  }


}
