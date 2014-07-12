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

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.util.Utils.Pair;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Pair<LOSS, GRADIENT> with LOSS: PAIR<SUM, EXAMPLE_COUNT>
 */
public class LossAndGradientReduceFunction implements ReduceFunction<Pair<Pair<Double, Integer>, Vector>> {

  private static final Logger LOG = Logger
      .getLogger(LossAndGradientReduceFunction.class.getName());

  @Inject
  public LossAndGradientReduceFunction() {
  }

  @Override
  public Pair<Pair<Double, Integer>, Vector> apply(final Iterable<Pair<Pair<Double, Integer>, Vector>> lags) {
    double lossSum = 0.0;
    int numEx = 0;
    Vector combinedGradient = null;
    for (final Pair<Pair<Double, Integer>, Vector> lag : lags) {
      if (combinedGradient == null) {
        combinedGradient = new DenseVector(lag.second.size());
      }
      lossSum += lag.first.first;
      numEx += lag.first.second;
      combinedGradient.add(lag.second);
    }
    return new Pair<>(new Pair<>(lossSum, numEx), combinedGradient);
  }

}
