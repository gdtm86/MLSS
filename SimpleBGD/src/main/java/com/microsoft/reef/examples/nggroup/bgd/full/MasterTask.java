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
package com.microsoft.reef.examples.nggroup.bgd.full;

import java.util.ArrayList;

import javax.inject.Inject;

import com.microsoft.reef.examples.nggroup.bgd.math.DenseVector;
import com.microsoft.reef.examples.nggroup.bgd.math.Vector;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ControlMessageBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.DescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LineSearchEvaluationsReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.LossAndGradientReducer;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.MinEtaBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelAndDescentDirectionBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.operatornames.ModelBroadcaster;
import com.microsoft.reef.examples.nggroup.bgd.parameters.AllCommunicationGroup;
import com.microsoft.reef.examples.nggroup.bgd.parameters.EnableRampup;
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
import com.microsoft.reef.io.network.nggroup.api.GroupChanges;
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
  private final Broadcast.Sender<Pair<Vector, Vector>> modelAndDescentDirectionBroadcaster;
  private final Broadcast.Sender<Vector> descentDriectionBroadcaster;
  private final Reduce.Receiver<Pair<Vector, Integer>> lineSearchEvaluationsReducer;
  private final Broadcast.Sender<Double> minEtaBroadcaster;
  private final boolean ignoreAndContinue;
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
      @Parameter(EnableRampup.class) final boolean rampup,
      final StepSizes ts) {
    this.lambda = lambda;
    this.maxIters = maxIters;
    this.ts = ts;
    this.ignoreAndContinue = rampup;
    this.model = new DenseVector(dimensions);
    this.communicationGroupClient = groupCommClient.getCommunicationGroup(AllCommunicationGroup.class);
    this.controlMessageBroadcaster = communicationGroupClient.getBroadcastSender(ControlMessageBroadcaster.class);
    this.modelBroadcaster = communicationGroupClient.getBroadcastSender(ModelBroadcaster.class);
    this.lossAndGradientReducer = communicationGroupClient.getReduceReceiver(LossAndGradientReducer.class);
    this.modelAndDescentDirectionBroadcaster = communicationGroupClient.getBroadcastSender(ModelAndDescentDirectionBroadcaster.class);
    this.descentDriectionBroadcaster = communicationGroupClient.getBroadcastSender(DescentDirectionBroadcaster.class);
    this.lineSearchEvaluationsReducer = communicationGroupClient.getReduceReceiver(LineSearchEvaluationsReducer.class);
    this.minEtaBroadcaster = communicationGroupClient.getBroadcastSender(MinEtaBroadcaster.class);
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

        final Pair<Vector, Integer> lineSearchEvals = lineSearch(descentDirection);

        updateModel(descentDirection, lineSearchEvals);

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

  private void updateModel(final Vector descentDirection, final Pair<Vector, Integer> lineSearchEvals) {
    try(Timer t = new Timer("GetDescentDirection + FindMinEta + UpdateModel")) {
      minEta = findMinEta(model, descentDirection, lineSearchEvals);
      descentDirection.scale(minEta);
      model.add(descentDirection);
    }

    System.out.println("New Model: " + model);
  }

  private Pair<Vector, Integer> lineSearch(final Vector descentDirection) throws NetworkException, InterruptedException {
    Pair<Vector, Integer> lineSearchEvals;
    do {
      try (Timer t = new Timer("LineSearch - Broadcast("
              + (sendModel ? "ModelAndDescentDirection" : "DescentDirection") + ") + Reduce(LossEvalsInLineSearch)")) {
        if (sendModel) {
          System.out.println("DoLineSearchWithModel");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearchWithModel);
          modelAndDescentDirectionBroadcaster.send(new Pair<>(model, descentDirection));
        } else {
          System.out.println("DoLineSearch");
          controlMessageBroadcaster.send(ControlMessages.DoLineSearch);
          descentDriectionBroadcaster.send(descentDirection);
        }
        lineSearchEvals = lineSearchEvaluationsReducer.reduce();
        System.out.println("LineSearchEvals: " + lineSearchEvals.first + " #ex: " + lineSearchEvals.second);
      }

      sendModel = chkAndUpdate();
    } while (!ignoreAndContinue && sendModel);
    return lineSearchEvals;
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
    do {
      try(Timer t = new Timer("Broadcast(" + (sendModel ? "Model" : "MinEta") + ") + Reduce(LossAndGradient)")) {
        if (sendModel) {
          System.out.println("ComputeGradientWithModel");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithModel);
          modelBroadcaster.send(model);
        } else {
          System.out.println("ComputeGradientWithMinEta");
          controlMessageBroadcaster.send(ControlMessages.ComputeGradientWithMinEta);
          minEtaBroadcaster.send(minEta);
        }
        lossAndGradient = lossAndGradientReducer.reduce();
        System.out.println("Loss: " + lossAndGradient.first.first + " #ex: " + lossAndGradient.first.second);
        System.out.println("Gradient: " + lossAndGradient.second + " #ex: " + lossAndGradient.first.second);
      }
      sendModel = chkAndUpdate();
    } while (!ignoreAndContinue && sendModel);
    return lossAndGradient;
  }

  /**
   * @return
   */
  private boolean chkAndUpdate() {
    long t1 = System.currentTimeMillis();
    final GroupChanges changes = communicationGroupClient.getTopologyChanges();
    long t2 = System.currentTimeMillis();
    System.out.println("Time to get TopologyChanges = " + (t2 - t1) / 1000.0 + " sec");
    if (changes.exist()) {
      System.out.println("There exist topology changes. Asking to update Topology");
      t1 = System.currentTimeMillis();
      communicationGroupClient.updateTopology();
      t2 = System.currentTimeMillis();
      System.out.println("Time to get TopologyUpdated = " + (t2 - t1) / 1000.0 + " sec");
      return true;
    } else {
      System.out.println("No changes in topology exist. So not updating topology");
      return false;
    }
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
   * @param lineSearchEvals
   * @return
   */
  private double findMinEta(final Vector model, final Vector descentDir, final Pair<Vector, Integer> lineSearchEvals) {
    final double wNormSqr = model.norm2Sqr();
    final double dNormSqr = descentDir.norm2Sqr();
    final double wDotd = model.dot(descentDir);
    final double[] t = ts.getT();
    int i = 0;
    for (final double eta : t) {
      final double modelNormSqr = wNormSqr + (eta * eta) * dNormSqr + 2 * eta * wDotd;
      final double loss = regularizeLoss(lineSearchEvals.first.get(i), lineSearchEvals.second, modelNormSqr);
      lineSearchEvals.first.set(i, loss);
      ++i;
    }
    System.out.println("Regularized LineSearchEvals: " + lineSearchEvals.first);
    final Tuple<Integer, Double> minTup = lineSearchEvals.first.min();
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
    gradient.scale(-1);
    System.out.println("DescentDirection: " + gradient);
    return gradient;
  }

}
