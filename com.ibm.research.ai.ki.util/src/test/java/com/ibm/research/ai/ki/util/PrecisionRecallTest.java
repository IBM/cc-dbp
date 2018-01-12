/**
 * cc-dbp-dataset
 *
 * Copyright (c) 2017 IBM
 *
 * The author licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.research.ai.ki.util;

import static org.junit.Assert.*;

import java.util.*;

import com.ibm.research.ai.ki.util.eval.*;

import org.junit.*;

public class PrecisionRecallTest {
	
	@Test
	public void testROC() {
		//test that areaROC is equal to the probability that a positive is scored higher than a negative
		for (int testi = 0; testi < 4; ++testi) {
			int negAbovePosPer10 = testi+1;
			
			PrecisionRecall pr = new PrecisionRecall();
			for (int i = 0; i < 100; ++i) {
				pr.addAnswered(null, 0.7, true);
				for (int ni = 0; ni < 10-negAbovePosPer10; ++ni) {
					pr.addAnswered(null, 0.3, false);
				}
				for (int ni = 0; ni < negAbovePosPer10; ++ni) {
					pr.addAnswered(null, 0.9, false);
				}
			}
			assertEquals(pr.computeSummaryScores().areaROC, 1.0-(double)negAbovePosPer10/10.0, 0.00001);
		}
	}
	
	@Test
	public void testWeighted() {
		//test that weighted instances work the same as repeating the instance
		Random rand = new Random(123);
		for (int testi = 0; testi < 10; ++testi) {
			PrecisionRecall prWeights = new PrecisionRecall();
			PrecisionRecall prRepeats = new PrecisionRecall();
			for (int i = 0; i < 100; ++i) {
				boolean isRel = rand.nextBoolean();
				double score = rand.nextDouble() + (isRel ? rand.nextDouble() : 0.0);
				int weight = rand.nextInt(9) + 1;
				prWeights.addAnswered(null, score, isRel, weight);
				for (int rep = 0; rep < weight; ++rep) {
					prRepeats.addAnswered(null, score, isRel);
				}
			}
			PrecisionRecall.SummaryScores ssW = prWeights.computeSummaryScores();
			PrecisionRecall.SummaryScores ssR = prRepeats.computeSummaryScores();
			assertEquals(ssW.areaROC, ssR.areaROC, 0.00001);
			assertEquals(ssW.auc, ssR.auc, 0.0001);
			assertEquals(ssW.maxFScore, ssR.maxFScore, 0.00001);
		}
	}
}
