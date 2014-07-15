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
package com.microsoft.reef.examples.nggroup.bgd.utils;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.io.network.group.operators.Reduce.ReduceFunction;
import com.microsoft.reef.io.network.util.Utils.Pair;

import javax.inject.Inject;

/**
 *
 */
public class LineSearchReduceFunction implements ReduceFunction<Pair<Vector, Integer>> {

  @Inject
  public LineSearchReduceFunction() {
  }

  @Override
  public Pair<Vector, Integer> apply(final Iterable<Pair<Vector, Integer>> evals) {
    Vector combinedEvaluations = null;
    int numEx = 0;
    for (final Pair<Vector, Integer> eval : evals) {
      if (combinedEvaluations == null) {
        combinedEvaluations = new DenseVector(eval.first.size());
      }
      combinedEvaluations.add(eval.first);
      numEx += eval.second;
    }
    return new Pair<>(combinedEvaluations, numEx);
  }

}
