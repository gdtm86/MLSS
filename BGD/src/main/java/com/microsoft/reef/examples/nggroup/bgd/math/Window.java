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

import java.util.ArrayList;
import java.util.List;


// TODO: Document
public class Window {
  private final int maxSize;
  private final List<Double> list;

  public Window(int size) {
    this.maxSize = size;
    list = new ArrayList<>(size);
  }

  public void add(double d) {
    if (list.size() < maxSize) {
      list.add(d);
      return;
    }
    list.remove(0);
    list.add(d);
  }

  public double avg() {
    if (list.size() == 0)
      return 0;
    double retVal = 0;
    for (double d : list) {
      retVal += d;
    }
    return retVal / list.size();
  }

  public double avgIfAdded(double d) {
    if (list.isEmpty())
      return d;
    int start = (list.size() < maxSize) ? 0 : 1;
    int numElems = (list.size() < maxSize) ? list.size() + 1 : maxSize;
    for (int i = start; i < list.size(); i++)
      d += list.get(i);
    return d / numElems;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Window w = new Window(3);
    for (int i = 1; i < 10; i++) {
      double exp = w.avgIfAdded(i);
      w.add(i);
      double act = w.avg();
      System.out.println("Exp: " + exp + " Act: " + act);
    }

  }

}
