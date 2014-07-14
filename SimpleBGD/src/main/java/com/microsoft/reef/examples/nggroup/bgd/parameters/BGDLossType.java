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
package com.microsoft.reef.examples.nggroup.bgd.parameters;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.loss.LogisticLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.SquaredErrorLossFunction;
import com.microsoft.reef.examples.nggroup.bgd.loss.WeightedLogisticLossFunction;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class BGDLossType {

  private final Class<? extends LossFunction> lossFunction;

  @Inject
  public BGDLossType(@Parameter(LossFunctionType.class) final String lossFunctionStr) {
    switch(lossFunctionStr) {
    case "logLoss":
      this.lossFunction = LogisticLossFunction.class;
      break;
    case "weightedLogLoss":
      this.lossFunction = WeightedLogisticLossFunction.class;
      break;
    case "squaredError":
      this.lossFunction = SquaredErrorLossFunction.class;
      break;
    default:
      throw new RuntimeException("Specified loss function type: " + lossFunctionStr
              + " is not yet implemented. Supported types are logLoss|weightedLogLoss|squaredError");
    }
  }

  /**
   * @return the lossFunction
   */
  public Class<? extends LossFunction> getLossFunction() {
    return lossFunction;
  }
}
