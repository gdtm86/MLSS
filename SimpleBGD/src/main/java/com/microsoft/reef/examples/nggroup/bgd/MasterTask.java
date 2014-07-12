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

import com.microsoft.reef.examples.nggroup.bgd.ControlMessages;
import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.*;
import com.microsoft.reef.examples.nggroup.bgd.parameters.*;
import com.microsoft.reef.examples.nggroup.bgd.utils.StepSizes;
import com.microsoft.reef.examples.nggroup.bgd.utils.Timer;
import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.Tuple;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
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

  private final CommunicationGroupClient communicationGroupClient;
  private final Broadcast.Sender<ControlMessages> controlMessageBroadcaster;
  private final Broadcast.Sender<Vector> modelBroadcaster;
  private final Reduce.Receiver<Pair<Pair<Double, Integer>, Vector>> lossAndGradientReducer;
  private final double lambda;
  private double eta;
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
      @Parameter(Eta.class) final double eta,
      @Parameter(Iterations.class) final int maxIters,
      @Parameter(EnableRampup.class) final boolean rampup) {
    this.lambda = lambda;
    this.eta = eta;
    this.maxIters = maxIters;
    this.model = new DenseVector(dimensions);
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {


    Vector gradient;
    try(Timer t = new Timer("Startup(including dataload) and Evaluation of initial model")) {
      gradient = evaluateCurrentModel();
    }

    int iters = 1;
    while (!converged(iters, gradient.norm2())) {
      try(Timer t = new Timer("Current Iteration(" + (iters) + ")")) {

        final Vector descentDirection = getDescentDirection(gradient);

        updateModel(descentDirection, iters);

        gradient = evaluateCurrentModel();

        ++iters;
      }
    }
    System.out.println("Stop");
    controlMessageBroadcaster.send(ControlMessages.Stop);

    for (final Double loss : losses) {
      System.out.println(loss);
    }
    return lossCodec.encode(losses);
  }

  private Vector evaluateCurrentModel() throws NetworkException, InterruptedException {
    final Pair<Pair<Double, Integer>, Vector> lossAndGradient = computeLossAndGradient();

    final Vector gradient = regularizeLossAndGradient(lossAndGradient);
    return gradient;
  }

  private void updateModel(final Vector descentDirection, final int iter) {
    try(Timer t = new Timer("UpdateModel")) {
      descentDirection.scale(eta);
      model.add(descentDirection);
      eta *= 0.99;
    }

    System.out.println("New Model: " + model);
  }

  private Vector regularizeLossAndGradient(final Pair<Pair<Double, Integer>, Vector> lossAndGradient) {
    Vector gradient;
    try(Timer t = new Timer("Regularize(Loss) + Regularize(Gradient)")) {
      final double loss = regularizeLoss(lossAndGradient.first.first, lossAndGradient.first.second, model);
      System.out.println("RegLoss: " + loss);
      gradient = regularizeGrad(lossAndGradient.second, lossAndGradient.first.second, model);
      System.out.println("RegGradient: " + gradient);
      losses.add(loss);
    }
    return gradient;
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
  private Pair<Pair<Double,Integer>,Vector> computeLossAndGradient() throws NetworkException, InterruptedException {
    Pair<Pair<Double, Integer>, Vector> lossAndGradient = null;
    try(Timer t = new Timer("Broadcast(Model) + Reduce(LossAndGradient)")) {
      System.out.println("ComputeGradientWithModel");
      controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithModel);
      modelBroadcaster.send(model);
      lossAndGradient = lossAndGradientReducer.reduce();
      System.out.println("Loss: " + lossAndGradient.first.first + " #ex: " + lossAndGradient.first.second);
      System.out.println("Gradient: " + lossAndGradient.second + " #ex: " + lossAndGradient.first.second);
    }
    return lossAndGradient;
  }

  /**
   * @param second
   * @param model
   * @param second2
   * @return
   */
  private Vector regularizeGrad(final Vector gradient, final int numEx, final Vector model) {
    gradient.scale(1.0 / numEx);
    gradient.multAdd(lambda, model);
    return gradient;
  }

  /**
   * @param first
   * @param model
   * @return
   */
  private double regularizeLoss(final double loss, final int numEx, final Vector model) {
    return regularizeLoss(loss, numEx, model.norm2Sqr());
  }

  private double regularizeLoss(final double loss, final int numEx, final double modelNormSqr) {
    return loss / numEx + ((lambda / 2) * modelNormSqr);
  }

  /**
   * @param loss
   * @return
   */
  private boolean converged(final int iters, final double gradNorm) {
    return iters >= maxIters || Math.abs(gradNorm) <= 1e-3;
  }

  /**
   * @param gradient
   * @return
   */
  private Vector getDescentDirection(final Vector gradient) {
    gradient.scale(-1);
    System.out.println("DescentDirection: " + gradient);
    return gradient;
  }

}
