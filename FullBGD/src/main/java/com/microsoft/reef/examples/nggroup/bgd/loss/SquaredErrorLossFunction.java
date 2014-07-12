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

/**
 * The Squarred Error {@link LossFunction}.
 *
 * @author Markus Weimer <mweimer@microsoft.com>
 */
public class SquaredErrorLossFunction implements LossFunction {

  /**
   * Trivial constructor.
   */
  @Inject
  public SquaredErrorLossFunction() {
  }

  @Override
  public double computeLoss(double y, double f) {
    return Math.pow(y - f, 2.0);
  }

  @Override
  public double computeGradient(double y, double f) {
    return (f - y) * 0.5;
  }

  @Override
  public String toString() {
    return "SquaredErrorLossFunction{}";
  }
}
