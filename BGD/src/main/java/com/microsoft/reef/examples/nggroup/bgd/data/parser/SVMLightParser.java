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
package com.microsoft.reef.examples.nggroup.bgd.data.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.SparseExample;

/**
 * A Parser for SVMLight records
 */
public class SVMLightParser implements Parser<String> {

  @Inject
  public SVMLightParser() {
  }

  @Override
  public Example parse(final String line) {
    final int entriesCount = StringUtils.countMatches(line, ":");
    final int[] indices = new int[entriesCount];
    final float[] values = new float[entriesCount];

    final String[] entries = StringUtils.split(line, ' ');
    String labelStr = entries[0];

    final boolean pipeExists = labelStr.indexOf('|')!=-1;
    if(pipeExists) {
      labelStr = labelStr.substring(0, labelStr.indexOf('|'));
    }
    double label = Double.parseDouble(labelStr);

    if (label != 1) {
      label = -1;
    }

    for (int j = 1; j < entries.length; ++j) {
      final String x = entries[j];
      final String[] entity = StringUtils.split(x, ':');
      final int offset = pipeExists ? 0 : 1;
      indices[j - 1] = Integer.parseInt(entity[0]) - offset;
      values[j - 1] = Float.parseFloat(entity[1]);
    }
    return new SparseExample(label, values, indices);
  }

  public static void main(final String[] args) {
    final Parser<String> parser = new SVMLightParser();
    for (int i = 0; i < 10; i++) {
      final List<SparseExample> examples = new ArrayList<>();
      float avgFtLen = 0;
      try (BufferedReader br = new BufferedReader(new FileReader(
          "C:\\Users\\shravan\\data\\splice\\hdi\\hdi_uncomp\\part-r-0000" + i))) {
        String line = null;
        while ((line = br.readLine()) != null) {
          final SparseExample spEx = (SparseExample) parser.parse(line);
          avgFtLen += spEx.getFeatureLength();
          examples.add(spEx);
        }
      } catch (final IOException e) {
        throw new RuntimeException("Exception", e);
      }
      System.out.println(examples.size() + " " + avgFtLen + " " + avgFtLen
          / examples.size());
      examples.clear();
    }
  }
}
