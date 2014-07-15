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
package com.microsoft.reef.examples.nggroup.bgd.data;

import com.microsoft.reef.examples.nggroup.bgd.math.Vector;

/**
 * Example implementation on a index and value array.
 */
public final class SparseExample implements Example {

  /**
   *
   */
  private static final long serialVersionUID = -2127500625316875426L;
  private final float[] values;
  private final int[] indices;
  private final double label;

  public SparseExample(final double label, final float[] values, final int[] indices) {
    this.label = label;
    this.values = values;
    this.indices = indices;

  }

  public int getFeatureLength() {
    return values.length;
  }

  @Override
  public double getLabel() {
    return this.label;
  }

  @Override
  public double predict(final Vector w) {
    double result = 0.0;
    for (int i = 0; i < indices.length; ++i) {
      result += w.get(this.indices[i]) * this.values[i];
    }
    return result;
  }

  @Override
  public void addToGradient(final Vector gradientVector, final double gradient) {
    for (int i = 0; i < this.indices.length; ++i) {
      final int index = this.indices[i];
      final double contribution = gradient * this.values[i];
      final double oldValue = gradientVector.get(index);
      final double newValue = oldValue + contribution;
      gradientVector.set(index, newValue);
    }
  }
}
