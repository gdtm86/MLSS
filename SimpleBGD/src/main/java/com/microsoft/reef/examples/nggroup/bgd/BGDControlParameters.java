/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.bgd;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.loss.LogisticLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.WeightedLogisticLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.parameters.EnableRampup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Eps;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Eta;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.MinParts;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.formats.CommandLine;

/**
 *
 */
public class BGDControlParameters {

  private final int dimensions;
  private final double lambda;
  private final double eps;
  private final int iters;
  private final int minParts;
  private final boolean rampup;

  private final Class<? extends LossFunction> lossFunction = LogisticLossFunction.class;
  private final double eta;

  @Inject
  public BGDControlParameters(
          @Parameter(ModelDimensions.class) final int dimensions,
          @Parameter(Lambda.class) final double lambda,
          @Parameter(Eps.class) final double eps,
          @Parameter(Eta.class) final double eta,
          @Parameter(Iterations.class) final int iters,
          @Parameter(EnableRampup.class) final boolean rampup,
          @Parameter(MinParts.class) final int minParts) {
            this.dimensions = dimensions;
            this.lambda = lambda;
            this.eps = eps;
            this.eta = eta;
            this.iters = iters;
            this.rampup = rampup;
            this.minParts = minParts;
  }

  public Configuration getConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(ModelDimensions.class, Integer.toString(dimensions))
              .bindNamedParameter(Lambda.class, Double.toString(lambda))
              .bindNamedParameter(Eps.class, Double.toString(eps))
              .bindNamedParameter(Eta.class, Double.toString(eta))
              .bindNamedParameter(Iterations.class, Integer.toString(iters))
              .bindNamedParameter(EnableRampup.class, Boolean.toString(rampup))
              .bindNamedParameter(MinParts.class, Integer.toString(minParts))
              .build();
  }

  public static void registerShortNames(final CommandLine commandLine) {
    commandLine.registerShortNameOfClass(ModelDimensions.class);
    commandLine.registerShortNameOfClass(Lambda.class);
    commandLine.registerShortNameOfClass(Eps.class);
    commandLine.registerShortNameOfClass(Eta.class);
    commandLine.registerShortNameOfClass(Iterations.class);
    commandLine.registerShortNameOfClass(EnableRampup.class);
    commandLine.registerShortNameOfClass(MinParts.class);
  }

  public int getDimensions() {
    return dimensions;
  }

  public double getLambda() {
    return lambda;
  }

  public double getEps() {
    return eps;
  }

  public double getEta() {
    return eta;
  }

  public int getIters() {
    return iters;
  }

  public int getMinParts() {
    return minParts;
  }

  public boolean isRampup() {
    return rampup;
  }

  public Class<? extends LossFunction> getLossFunction() {
    return lossFunction;
  }


}
