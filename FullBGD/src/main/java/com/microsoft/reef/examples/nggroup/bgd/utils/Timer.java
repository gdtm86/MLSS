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
package com.microsoft.reef.examples.nggroup.bgd.utils;

public class Timer implements AutoCloseable {

  private final String description;
  private final long t1;

  public Timer(final String description) {
    this.description = description;
    System.out.println();
    System.out.println(description + " Starting:");
    t1 = System.currentTimeMillis();
  }

  @Override
  public void close() {
    final long t2 = System.currentTimeMillis();
    System.out.println();
    System.out.println(description + " Ended:");
    System.out.println(description + " took " + (t2 - t1) / 1000.0 + " sec");
    System.out.println();
  }

}