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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.microsoft.reef.examples.nggroup.bgd.data.Example;
import com.microsoft.reef.examples.nggroup.bgd.data.parser.Parser;
import com.microsoft.reef.examples.nggroup.bgd.loss.LossFunction;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.*;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.task.Task;

/**
 *
 */
public class SlaveTask implements Task {

  private static final Logger LOG = Logger.getLogger(SlaveTask.class.getName());

  private final Broadcast.Receiver<ControlMessages> controlMessageReceiver;
  private final Broadcast.Receiver<Vector> modelReceiver;
  private final Reduce.Sender<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final LossFunction lossFunction;
  private Vector model = null;

  @Inject
  public SlaveTask(final GroupCommClient groupCommClient,
                   final DataSet<LongWritable, Text> dataSet,
                   final Parser<String> parser,
                   final LossFunction lossFunction,
                   final StepSizes ts) {
    this.dataSet = dataSet;
    this.parser = parser;
    this.lossFunction = lossFunction;
    final CommunicationGroupClient communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    controlMessageReceiver = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    modelReceiver = communicationGroup.getBroadcastReceiver(ModelBroadcaster.class);
    lossAndGradientReducer = communicationGroup.getReduceSender(LossAndGradientReducer.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    boolean stopped = false;
    while (!stopped) {
      final ControlMessages controlMessage = controlMessageReceiver.receive();
      switch (controlMessage) {
        case Stop:
          stopped = true;
          break;

        case ComputeGradientWithModel:
          this.model = modelReceiver.receive();
          lossAndGradientReducer.send(computeLossAndGradient());
          break;

        default:
          break;
      }
    }
    return null;
  }

  /**
   * @param model
   * @return
   */
  private Pair<Pair<Double, Integer>, Vector> computeLossAndGradient() {
    if (examples.isEmpty()) {
      loadData();
    }
    final Vector gradient = new DenseVector(model.size());
    double loss = 0.0;
    for (final Example example : examples) {
      final double f = example.predict(model);

      final double g = this.lossFunction.computeGradient(example.getLabel(), f);
      example.addToGradient(gradient, g);
      loss += this.lossFunction.computeLoss(example.getLabel(), f);
    }
    return new Pair<>(new Pair<>(loss, examples.size()), gradient);
  }

  /**
   *
   */
  private void loadData() {
    LOG.info("Loading data");
    int i = 0;
    for (final Pair<LongWritable, Text> examplePair : dataSet) {
      final Example example = parser.parse(examplePair.second.toString());
      examples.add(example);
      if (++i % 2000 == 0) {
        LOG.info("Done parsing " + i + " lines");
      }
    }
  }

}
