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
package com.microsoft.reef.examples.nggroup.bgd.math;

import com.microsoft.reef.io.Tuple;

/**
 * Represents an immutable vector.
 */
public interface ImmutableVector {
  /**
   * Access the value of the Vector at dimension i
   *
   * @param i index
   * @return the value at index i
   */
  public double get(int i);

  /**
   * The size (dimensionality) of the Vector
   *
   * @return the size of the Vector.
   */
  public int size();

  /**
   * Computes the inner product with another Vector.
   *
   * @param that
   * @return the inner product between two Vectors.
   */
  public double dot(Vector that);

  /**
   * Computes the computeSum of all entries in the Vector.
   *
   * @return the computeSum of all entries in this Vector
   */
  public double sum();

  /**
   * Computes the L2 norm of this Vector.
   *
   * @return the L2 norm of this Vector.
   */
  public double norm2();

  /**
   * Computes the square of the L2 norm of this Vector.
   *
   * @return the square of the L2 norm of this Vector.
   */
  public double norm2Sqr();

  /**
   * Computes the min of all entries in the Vector
   *
   * @return the min of all entries in this Vector
   */
  public Tuple<Integer, Double> min();
}
