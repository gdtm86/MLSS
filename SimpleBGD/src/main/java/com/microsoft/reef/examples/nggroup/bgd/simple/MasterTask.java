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
package com.microsoft.reef.examples.nggroup.bgd.simple;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.*;
import com.microsoft.reef.examples.nggroup.bgd.parameters.*;
import com.microsoft.reef.examples.nggroup.bgd.utils.ControlMessages;
import com.microsoft.reef.examples.nggroup.bgd.utils.Timer;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupCommClient;
import com.microsoft.reef.io.network.util.Utils.Pair;
import com.microsoft.reef.io.serialization.Codec;
import com.microsoft.reef.io.serialization.SerializableCodec;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;

/**
 *
 */
public class MasterTask implements Task {

  private final Broadcast.Sender<ControlMessages> controlMessageSender;
  private final Broadcast.Sender<Vector> modelSender;
  private final Reduce.Receiver<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReceiver;
  private final double lambda;
  private double eta;
  private final int maxIters;
  private final ArrayList<Double> losses = new ArrayList<>();
  private final Codec<ArrayList<Double>> lossCodec = new SerializableCodec<ArrayList<Double>>();
  private final Vector model;


  @Inject
  public MasterTask(final GroupCommClient groupCommClient,
                    @Parameter(ModelDimensions.class) final int dimensions,
                    @Parameter(Lambda.class) final double lambda,
                    @Parameter(Eta.class) final double eta,
                    @Parameter(Iterations.class) final int maxIters,
                    @Parameter(EnableRampup.class) final boolean rampup) {
    this.lambda = lambda;
    this.eta = eta;
    this.maxIters = maxIters;
    this.model = new DenseVector(dimensions);
    final CommunicationGroupClient communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageSender = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelSender = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReceiver = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {

    final double gradientNorm = Double.MAX_VALUE;
    for (int iteration = 1; !converged(iteration, gradientNorm); ++iteration) {
      try (final Timer t = new Timer("Current Iteration(" + (iteration) + ")")) {
        final Pair<Double, Vector> lossAndGrad = computeLossAndGradient();
        losses.add(lossAndGrad.first);
        updateModel(lossAndGrad.second);
      }
    }
    System.out.println("Stop");
    controlMessageSender.send(ControlMessages.Stop);

    for (final Double loss : losses) {
      System.out.println(loss);
    }
    return lossCodec.encode(losses);
  }

  private void updateModel(final Vector gradient) {
    try (Timer t = new Timer("UpdateModel")) {
      model.scale(1-lambda * eta);
      model.multAdd(-eta, gradient);
      eta *= 0.99;
    }

    System.out.println("New Model: " + model);
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
  private Pair<Double, Vector> computeLossAndGradient() throws NetworkException, InterruptedException {
    try (Timer t = new Timer("Broadcast(Model) + Reduce(LossAndGradient)")) {
      System.out.println("ComputeGradientWithModel");
      controlMessageSender.send(ControlMessages.ComputeGradientWithModel);
      modelSender.send(model);
      final Pair<Pair<Double, Integer>, Vector> lossAndGradient = lossAndGradientReceiver.reduce();

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
}
