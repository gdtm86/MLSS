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

  /**
   *
   */
  private static final double FAILURE_PROB = 0.001;
  private final CommunicationGroupClient communicationGroup;
  private final Broadcast.Receiver<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Receiver<Vector> modelBroadcaster;
  private final Reduce.Sender<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Receiver<Pair<Vector, Vector>> modelAndDescentDirectionBroadcaster;
  private final Broadcast.Receiver<Vector> descentDirectionBroadcaster;
  private final Reduce.Sender<Pair<Vector, Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Receiver<Double> minEtaBroadcaster;
  private final List<Example> examples = new ArrayList<>();
  private final DataSet<LongWritable, Text> dataSet;
  private final Parser<String> parser;
  private final LossFunction lossFunction;
  private final StepSizes ts;

  private Vector model = null;
  private Vector descentDirection = null;

  @Inject
  public SlaveTask(
      final GroupCommClient groupCommClient,
      final DataSet<LongWritable, Text> dataSet,
      final Parser<String> parser,
      final LossFunction lossFunction,
      final StepSizes ts) {
    this.dataSet = dataSet;
    this.parser = parser;
    this.lossFunction = lossFunction;
    this.ts = ts;
    communicationGroup = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    controlMessageBroadcaster = communicationGroup.getBroadcastReceiver(ControlMessageBroadcaster.class);
    modelBroadcaster = communicationGroup.getBroadcastReceiver(ModelBroadcaster.class);
    lossAndGradientReducer = communicationGroup.getReduceSender(LossAndGradientReducer.class);
    modelAndDescentDirectionBroadcaster = communicationGroup.getBroadcastReceiver(ModelAndDescentDirectionBroadcaster.class);
    this.descentDirectionBroadcaster = communicationGroup.getBroadcastReceiver(DescentDirectionBroadcaster.class);
    lineSearchEvaluationsReducer = communicationGroup.getReduceSender(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroup.getBroadcastReceiver(MinEtaBroadcaster.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    boolean stop = false;
    while (!stop) {
      final ControlMessages controlMessage = controlMessageBroadcaster.receive();
      switch (controlMessage) {
        case Stop:
          stop = true;
          break;

        case ComputeGradientWithModel:
          failPerhaps();
          this.model = modelBroadcaster.receive();
          lossAndGradientReducer.send(computeLossAndGradient());
          break;

        case ComputeGradientWithMinEta:
          failPerhaps();
          final double minEta = minEtaBroadcaster.receive();
          assert (descentDirection != null);
          this.descentDirection.scale(minEta);
          assert (model != null);
          this.model.add(descentDirection);
          lossAndGradientReducer.send(computeLossAndGradient());
          break;

        case DoLineSearch:
          failPerhaps();
          this.descentDirection = descentDirectionBroadcaster.receive();
          lineSearchEvaluationsReducer.send(lineSearchEvals());
          break;

        case DoLineSearchWithModel:
          failPerhaps();
          final Pair<Vector, Vector> modelAndDescentDir = modelAndDescentDirectionBroadcaster.receive();
          this.model = modelAndDescentDir.first;
          this.descentDirection = modelAndDescentDir.second;
          lineSearchEvaluationsReducer.send(lineSearchEvals());
          break;

        default:
          break;
      }
    }
    return null;
  }

  private void failPerhaps() {
    if (Math.random() < FAILURE_PROB) {
      throw new RuntimeException("Simulated Failure");
    }
  }

  /**
   * @param modelAndDescentDir
   * @return
   */
  private Pair<Vector, Integer> lineSearchEvals() {
    if (examples.isEmpty()) {
      loadData();
    }
    final Vector zed = new DenseVector(examples.size());
    final Vector ee = new DenseVector(examples.size());
    for (int i = 0; i < examples.size(); i++) {
      final Example example = examples.get(i);
      double f = example.predict(model);
      zed.set(i, f);
      f = example.predict(descentDirection);
      ee.set(i, f);
    }

    final double[] t = ts.getT();
    final Vector evaluations = new DenseVector(t.length);
    int i = 0;
    for (final double d : t) {
      double loss = 0;
      for (int j = 0; j < examples.size(); j++) {
        final Example example = examples.get(j);
        final double val = zed.get(j) + d * ee.get(j);
        loss += this.lossFunction.computeLoss(example.getLabel(), val);
      }
      evaluations.set(i++, loss);
    }
    return new Pair<>(evaluations, examples.size());
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
      example.addGradient(gradient, g);
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
      if(++i % 2000 == 0) {
        LOG.info("Done parsing " + i + " lines");
      }
    }
  }

}
