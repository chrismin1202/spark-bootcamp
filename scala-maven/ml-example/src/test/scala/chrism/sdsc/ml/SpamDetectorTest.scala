/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package chrism.sdsc.ml

import chrism.sdsc.TestSuite
import chrism.sdsc.spark.TestSparkSessionMixin
import org.apache.spark.ml.tuning.CrossValidatorModel

final class SpamDetectorTest extends TestSuite with TestSparkSessionMixin {

  test("training spam detection cross-validation model") {
    val datasets = SpamDetector.loadDatasets()
    val trainingDs = datasets.trainingDs
    val testDs = datasets.testDs

    // train and persist the model
    val trainedModel = SpamDetector.trainModel(trainingDs)

    // The model can be persisted and reused.
    trainedModel.write
      .overwrite()
      .save(SpamDetector.DefaultNaiveBayesModelPath)

    // Load the saved model (just to demo that trained models can be persisted and used later)
    val reloadedModel = CrossValidatorModel.load(SpamDetector.DefaultNaiveBayesModelPath)

    val predictions = reloadedModel.bestModel.transform(testDs)
    println(s"area under PR: ${reloadedModel.getEvaluator.evaluate(predictions)}")
  }
}
