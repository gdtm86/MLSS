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

import javax.inject.Inject;
import java.util.Arrays;

/**
 *
 */
public class StepSizes {
  private final double[] t;
  private final int gridSize = 21;

  @Inject
  public StepSizes() {
    this.t = new double[gridSize];
    final int mid = (gridSize / 2);
    t[mid] = 1;
    for (int i = mid - 1; i >= 0; i--) {
      t[i] = t[i + 1] / 2.0;
    }
    for (int i = mid + 1; i < gridSize; i++) {
      t[i] = t[i - 1] * 2.0;
    }
  }

  public double[] getT() {
    return t;
  }


  public int getGridSize() {
    return gridSize;
  }


  /**
   * @param args
   */
  public static void main(final String[] args) {
    // TODO Auto-generated method stub
    final StepSizes t = new StepSizes();
    System.out.println(Arrays.toString(t.getT()));
  }

}
