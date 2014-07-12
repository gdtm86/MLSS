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
package com.microsoft.reef.examples.nggroup.bgd.loss;

import javax.inject.Inject;

public final class WeightedLogisticLossFunction implements LossFunction {

  double pos = 0.0025;
  double neg = 0.9975;
  double posweight;
  double negweight;

  /**
   * Trivial constructor.
   */
  @Inject
  public WeightedLogisticLossFunction() {
    posweight = (this.pos + this.neg) / (2 * this.pos);
    negweight = (this.pos + this.neg) / (2 * this.neg);

  }

  @Override
  public double computeLoss(double y, double f) {
    double predictedTimesLabel = y * f;
    double weight = posweight;
    if (y == -1) {
      weight = negweight;
    }
    if (predictedTimesLabel >= 0) {
      return weight * Math.log(1 + Math.exp(-predictedTimesLabel));
    } else {
      return weight * (-predictedTimesLabel + Math.log(1 + Math.exp(predictedTimesLabel)));
    }
  }

  @Override
  public double computeGradient(double y, double f) {
    double predictedTimesLabel = y * f;
    double weight = posweight;
    if (y == -1) {
      weight = negweight;
    }

    double probability = 0;
    if (predictedTimesLabel >= 0) {
      probability = 1 / (1 + Math.exp(-predictedTimesLabel));
    } else {
      double ExpVal = Math.exp(predictedTimesLabel);
      probability = ExpVal / (1 + ExpVal);
    }

    return (probability - 1) * y * weight;
  }

  @Override
  public String toString() {
    return "WeightedLogisticLossFunction{}";
  }
}


