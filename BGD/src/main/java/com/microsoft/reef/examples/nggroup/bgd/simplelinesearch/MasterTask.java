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
package com.microsoft.reef.examples.nggroup.bgd.simplelinesearch;

import java.util.ArrayList;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.DescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.MinEtaBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Iterations;
import com.microsoft.reef.examples.nggroup.bgd.parameters.Lambda;
import com.microsoft.reef.examples.nggroup.bgd.parameters.ModelDimensions;
import com.microsoft.reef.examples.nggroup.bgd.utils.ControlMessages;
import com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes;
import com.microsoft.reef.examples.nggroup.bgd.utils.Timer;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

/**
 *
 */
public class MasterTask implements Task {

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final Broadcast.Sender<Vector> descentDriectionBroadcaster;
  private final Reduce.Receiver<Pair<Vector, Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Sender<Double> minEtaBroadcaster;
  private final com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes ts;
  private final double lambda;
  private final int maxIters;
  final ArrayList<Double> losses = new ArrayList<>();
  final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
  private final Vector model;

  boolean sendModel = true;
  double minEta = 0;

  @Inject
  public MasterTask(
      final GroupCommClient groupCommClient,
      @Parameter(ModelDimensions.class) final int dimensions,
      @Parameter(Lambda.class) final double lambda,
      @Parameter(Iterations.class) final int maxIters,
      final StepSizes ts) {
    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ts = ts;
    this.model = new DenseVector(dimensions);
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
    this.descentDriectionBroadcaster = communicationGroupClient.getBroadcastSender(DescentDirectionBroadcaster.class);
    this.lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroupClient.getBroadcastSender(MinEtaBroadcaster.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    double gradientNorm = Double.MAX_VALUE;
    for (int iteration = 1; !converged(iteration, gradientNorm); ++iteration) {
      try(Timer t = new Timer("Current Iteration(" + (iteration) + ")")) {
        final Pair<Double,Vector> lossAndGradient = computeLossAndGradient();
        losses.add(lossAndGradient.first);
        final Vector descentDirection = getDescentDirection(lossAndGradient.second);

        updateModel(descentDirection);

        gradientNorm = descentDirection.norm2();
      }
    }
    System.out.println("Stop");
    controlMessageBroadcaster.send(ControlMessages.Stop);

    for (final Double loss : losses) {
      System.out.println(loss);
    }
    return lossCodec.encode(losses);
  }

  private void updateModel(final Vector descentDirection) throws NetworkException, InterruptedException {
    try(Timer t = new Timer("GetDescentDirection + FindMinEta + UpdateModel")) {
      final Vector lineSearchEvals = lineSearch(descentDirection);
      minEta = findMinEta(model, descentDirection, lineSearchEvals);
      model.multAdd(minEta, descentDirection);
    }

    System.out.println("New Model: " + model);
  }

  private Vector lineSearch(final Vector descentDirection) throws NetworkException, InterruptedException {
    try (Timer t = new Timer("LineSearch - Broadcast(DescentDirection) + Reduce(LossEvalsInLineSearch)")) {

      System.out.println("DoLineSearch");
      controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
      descentDriectionBroadcaster.send(descentDirection);

      final Pair<Vector, Integer> lineSearchEvals = lineSearchEvaluationsReducer.reduce();
      final int numExamples = lineSearchEvals.second;
      System.out.println("#Examples: " + numExamples);
      final Vector lineSearchResults = lineSearchEvals.first;
      lineSearchResults.scale(1.0/numExamples);
      System.out.println("LineSearchEvals: " + lineSearchResults);
      return lineSearchResults;
    }
  }

  /**
   * @param minEta
   * @param model
   * @param sendModel
   * @param lossAndGradient
   * @return
   * @throws InterruptedException
   * @throws NetworkException
   */
  private Pair<Double,Vector> computeLossAndGradient() throws NetworkException, InterruptedException {
    try(Timer t = new Timer("Broadcast(" + (sendModel ? "Model" : "MinEta") + ") + Reduce(LossAndGradient)")) {
      if (sendModel) {
        System.out.println("ComputeGradientWithModel");
        controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithModel);
        modelBroadcaster.send(model);
        sendModel = false;
      } else {
        System.out.println("ComputeGradientWithMinEta");
        controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithMinEta);
        minEtaBroadcaster.send(minEta);
      }
      final Pair<Pair<Double, Integer>, Vector> lossAndGradient = lossAndGradientReducer.reduce();
      final int numExamples = lossAndGradient.first.second;
      System.out.println("#Examples: " + numExamples);
      final double lossPerExample = lossAndGradient.first.first / numExamples;
      System.out.println("Loss: " + lossPerExample);
      final double objFunc = ((lambda / 2) * model.norm2Sqr()) + lossPerExample;
      System.out.println("Objective Func Value: " + objFunc);
      final Vector gradient = lossAndGradient.second;
      gradient.scale(1.0/numExamples);
      System.out.println("Gradient: " + gradient);
      return new Pair<>(objFunc, gradient);
    }
  }

  /**
   * @param loss
   * @return
   */
  private boolean converged(final int iters, final double gradNorm) {
    return iters >= maxIters || Math.abs(gradNorm) <= 1e-3;
  }

  /**
   * @param lineSearchEvals
   * @return
   */
  private double findMinEta(final Vector model, final Vector descentDir, final Vector lineSearchEvals) {
    final double wNormSqr = model.norm2Sqr();
    final double dNormSqr = descentDir.norm2Sqr();
    final double wDotd = model.dot(descentDir);
    final double[] t = ts.getT();
    int i = 0;
    for (final double eta : t) {
      final double modelNormSqr = wNormSqr + (eta * eta) * dNormSqr + 2 * eta * wDotd;
      final double loss = lineSearchEvals.get(i) + ((lambda / 2) * modelNormSqr);
      lineSearchEvals.set(i, loss);
      ++i;
    }
    System.out.println("Regularized LineSearchEvals: " + lineSearchEvals);
    final Tuple<Integer, Double> minTup = lineSearchEvals.min();
    System.out.println("MinTup: " + minTup);
    final double minT = t[minTup.getKey()];
    System.out.println("MinT: " + minT);
    return minT;
  }

  /**
   * @param gradient
   * @return
   */
  private Vector getDescentDirection(final Vector gradient) {
    gradient.multAdd(lambda, model);
    gradient.scale(-1);
    System.out.println("DescentDirection: " + gradient);
    return gradient;
  }

}
