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
package com.ibm.research.ai.ki.util.eval;

import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * Supports weighted instances and ids. Weighted instances right now are just used for correcting for negative downsampling in test.
 * The ids are to integrate better with the significance testing.
 * @author mrglass
 *
 */
public class PrecisionRecall implements Serializable {
	private static final long serialVersionUID = 1L;

	public static class Instance implements Comparable<Instance>, Serializable {
		private static final long serialVersionUID = 1L;
		public Instance(String id, double score, boolean relevant, double weight) {
			this.id = id;
			this.score = score;
			this.relevant = relevant;
			this.weight = weight;
		}
		public String id;
		public double score;
		public boolean relevant;
		public double weight;
		@Override
		public int compareTo(Instance o) {
			return (int)Math.signum(o.score - this.score);
		}
	}

	/**
	 * Much like the version in PrecisionRecallThreshold, but less junk.
	 * @author mrglass
	 *
	 */
	public static class SummaryScores {
		public double maxFScore;
		public double maxFScoreThreshold;
		public double maxFScorePrecision;
		public double maxFScoreRecall;
		public double auc;

		public double fScore5;

		public double areaROC;
		
		public double positiveCount;
		public double totalCount;
		
		public double outOfRecall;
		
		public static int[] precisionAtPoints = new int[] {5, 10, 20, 50, 100, 500, 1000};
		public double[] precisionAt; 

		// methods for computing micro-F1
		public double getTruePositive() {
			return (maxFScoreRecall * positiveCount);
		}

		public double getFalseNegative() {
			return (positiveCount - getTruePositive());
		}

		public double getFalsePositive() {
			double tp = (maxFScoreRecall * positiveCount);
			if (tp == 0)
				return 0; // then the F-Score maximizing threshold is inf.
			// prec = tp/(tp+fp)
			// 1/prec = (tp+fp)/tp
			// tp/prec = tp+fp
			// tp/prec - tp = fp
			return (tp / maxFScorePrecision - tp);
		}

		/**
		 * P/R AUC weighted by number of positives
		 * @param scores
		 * @return
		 */
		public static double microAUC(Iterable<SummaryScores> scores) {
			double sumAUC = 0;
			double sumPositiveCount = 0;
			for (SummaryScores score : scores) {
				sumAUC += score.auc * score.positiveCount;
				sumPositiveCount += score.positiveCount;
			}
			return sumAUC / sumPositiveCount;
		}
		
		/**
		 * P/R AUC average, with positive counts less than minPositives ignored
		 * @param scores
		 * @param minPositives
		 * @return
		 */
		public static double macroAUC(Iterable<SummaryScores> scores, double minPositives) {
			double sumAUC = 0;
			double count = 0;
			for (SummaryScores score : scores) {
				if (score.positiveCount < minPositives)
					continue;
				sumAUC += score.auc;
				count += 1;
			}
			return sumAUC / count;
		}
		
		/**
		 * F1 built from summing true/false positive/negative across all classes
		 * at F1 maximizing threshold
		 * 
		 * @param scores
		 * @return
		 */
		public static double microF1(Iterable<SummaryScores> scores) {
			double sumTP = 0;
			double sumFP = 0;
			double sumFN = 0;

			for (SummaryScores score : scores) {
				sumTP += score.getTruePositive();
				sumFP += score.getFalsePositive();
				sumFN += score.getFalseNegative();
			}

			return 2.0 * sumTP / (2.0 * sumTP + sumFN + sumFP);
		}

		/**
		 * Plain average of max F1 scores
		 * 
		 * @param scores
		 * @return
		 */
		public static double macroF1(Iterable<SummaryScores> scores) {
			double sumF1 = 0;
			int count = 0;
			for (SummaryScores score : scores) {
				sumF1 += score.maxFScore;
				++count;
			}
			return sumF1 / count;
		}

		public String toString() {
			return "Max F-Score = " + maxFScore + " at " + maxFScoreThreshold + " with P/R of "
						+ maxFScorePrecision + " and " + maxFScoreRecall + 
						"\nAuC = " + auc + ", ROC Area = "+areaROC+", F@0.5:" + fScore5 + 
						"\nNumber of positive = " + positiveCount + " of " + totalCount;
		}
	}
	
	protected List<Instance> instances = new ArrayList<>();
	
	public void addAnswered(Instance inst) {
		instances.add(inst);
	}
	
	public void addAnswered(String id, double score, boolean relevant) {
		addAnswered(id, score, relevant, 1.0);
	}
	
	public void addAnswered(String id, double score, boolean relevant, double weight) {
		instances.add(new Instance(id, score, relevant, weight));
	}
	
	public SummaryScores computeSummaryScores() {
		return computeSummaryScores(1.0);
	}
	
	public SummaryScores computeSummaryScores(double betaForFScore) {
		double betaForFScoreSqr = betaForFScore*betaForFScore;
		Random rand = new Random(123);
		SummaryScores sum = new SummaryScores();
		sum.precisionAt = new double[SummaryScores.precisionAtPoints.length];
		Collections.shuffle(instances, rand);
		Collections.sort(instances);

		//CONSIDER: include these in summary scores?
		double inRecallPositiveCount = 0.0;
		double inRecallTotalCount = 0.0;
		for (Instance i : instances) {
			if (i.relevant)
				sum.positiveCount += i.weight;
			sum.totalCount += i.weight;
			if (i.score != Double.NEGATIVE_INFINITY) {
				if (i.relevant)
					inRecallPositiveCount += i.weight;
				inRecallTotalCount += i.weight;
			}
		}

		boolean pastPoint5 = false;
		double cummulativeCorrect = 0.0;
		double cummulativeTotal = 0.0;
		for (int i = 0; i < instances.size(); ++i) {
			Instance inst = instances.get(i);
			if (inst.score == Double.NEGATIVE_INFINITY)
				break;
			cummulativeTotal += inst.weight;
			if (inst.relevant) {
				cummulativeCorrect += inst.weight;
				double precisionBefore = cummulativeTotal == inst.weight ? 
						1.0 : 
						(cummulativeCorrect-inst.weight) / (cummulativeTotal-inst.weight);
				double precisionAfter = cummulativeCorrect / cummulativeTotal;
				sum.auc += (precisionBefore + precisionAfter)/2 * (inst.weight/sum.positiveCount); //precision x recall-increment
			}

			double tp = cummulativeCorrect;
			double fp = cummulativeTotal - cummulativeCorrect;
			double fn = sum.positiveCount - cummulativeCorrect;
			double precision = (double) (tp) / (tp + fp);
			double recall = (double) (tp) / (tp + fn);
			double f1 = (1+betaForFScoreSqr) * precision * recall / (betaForFScoreSqr*precision + recall);
			
			if (inst.weight != 1.0)
			    sum.precisionAt = null; //can't handle weights with precisionAt
			if (sum.precisionAt != null) {
    			int pAt = Arrays.binarySearch(SummaryScores.precisionAtPoints, i+1);
    			if (pAt >= 0) {
    			    sum.precisionAt[pAt] = precision;
    			}
			}
			
			//for ROC
			double tpRate = tp / inRecallPositiveCount;
			double fpRate = fp / (inRecallTotalCount - inRecallPositiveCount);
			if (!inst.relevant) {
				//is it correct for areaROC to ignore the out-of-recall?
				sum.areaROC += tpRate * inst.weight/(inRecallTotalCount - inRecallPositiveCount); //recall x false-pos-increment
			}
			
			double nextConfidence = (i+1) == instances.size() ? 0d : instances.get(i+1).score;
			if (!pastPoint5 && nextConfidence < 0.5) {
				sum.fScore5 = f1;
				pastPoint5 = true;
			}

			if (f1 > sum.maxFScore) {
				sum.maxFScore = f1;
				sum.maxFScoreThreshold = inst.score;
				sum.maxFScorePrecision = precision;
				sum.maxFScoreRecall = recall;
			}
		}

		sum.outOfRecall = sum.totalCount - cummulativeTotal;
		
		return sum;
	}
	
	public int size() {
		return instances.size();
	}
	
	public void clear() {
		instances.clear();
	}
	
	public void addOutOfRecall(int outOfRecallCount) {
		for (int i = 0; i < outOfRecallCount; ++i) {
			//CONSIDER: or gensym id?
			instances.add(new Instance(null, Double.NEGATIVE_INFINITY, true, 1.0));
		}
	}
	
	public void addOutOfRecall(String id, double weight) {
		instances.add(new Instance(id, Double.NEGATIVE_INFINITY, true, weight));
	}
	
	/**
	 * Get results in a form useable with SamplingPermutationTest
	 * @return
	 */
	public Map<String,Instance> resultsForSignificance() {
		Map<String,Instance> results = new HashMap<>();
		for (Instance inst : instances) {
			if (results.put(inst.id, inst) != null) 
				throw new IllegalArgumentException("Duplicate ids, not suitable for significance testing.");
		}
		return results;
	}
	

	/**
	 * Scorer for use with SamplingPermutationTest, or BootstrappingConfidenceInterval
	 * @author mrglass
	 *
	 */
	public static class AUCScore implements Function<Collection<Instance>, Double> {
		public Double apply(Collection<Instance> insts) {
			ArrayList<Instance> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Instance>)insts;
			} else {
				pr = new ArrayList<Instance>(insts);
			}
			Collections.sort(pr);

			double positiveCount = 0.0;
			for (Instance i : pr) {
				if (i.relevant)
					positiveCount += i.weight;
			}

			double auc = 0.0;
			double cummulativeCorrect = 0.0;
			double cummulativeTotal = 0.0;
			for (Instance inst : pr) {
				if (inst.score == Double.NEGATIVE_INFINITY)
					break;
				cummulativeTotal += inst.weight;
				if (inst.relevant) {
					cummulativeCorrect += inst.weight;
					auc += cummulativeCorrect / (cummulativeTotal * positiveCount);
				}
			}
			return auc;
		}
	}	
	
	/**
	 * Scorer for use with SamplingPermutationTest, or BootstrappingConfidenceInterval
	 * @author mrglass
	 *
	 */
	public static class MaxFScore implements Function<Collection<Instance>, Double> {
		public MaxFScore(double betaForFScore) {
			this.betaForFScore = betaForFScore;
		}
		public MaxFScore() {
			this(1.0);
		}
		
		final double betaForFScore;
		
		public Double apply(Collection<Instance> insts) {
			double betaForFScoreSqr = betaForFScore*betaForFScore;
			
			ArrayList<Instance> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Instance>)insts;
			} else {
				pr = new ArrayList<Instance>(insts);
			}
			Collections.sort(pr);

			double positiveCount = 0.0;
			for (Instance i : pr) {
				if (i.relevant)
					positiveCount += i.weight;
			}

			double maxF = 0.0;
			double cummulativeCorrect = 0.0;
			double cummulativeTotal = 0.0;
			for (Instance inst : pr) {
				if (inst.score == Double.NEGATIVE_INFINITY)
					break;
				cummulativeTotal += inst.weight;
				if (inst.relevant) {
					cummulativeCorrect += inst.weight;
					
					double tp = cummulativeCorrect;
					double fp = cummulativeTotal - cummulativeCorrect;
					double fn = positiveCount - cummulativeCorrect;
					double precision = (double) (tp) / (tp + fp);
					double recall = (double) (tp) / (tp + fn);
					double f1 = (1+betaForFScoreSqr) * precision * recall / (betaForFScoreSqr*precision + recall);
					
					if (f1 > maxF) {
						maxF = f1;
					}
				}
			}
			return maxF;
		}
	}
	
	   public String tsvPRCurve() {
	        return tsvPRCurve(-1);
	    }
	    
	    public String tsvPRCurve(int approxDesiredPoints) {
	        Collections.shuffle(instances, new Random(123));
	        Collections.sort(instances);
	        double allPositive = 0;
	        for (Instance p : instances) {
	            if (p.relevant)
	                allPositive += 1;
	        }
	        
	        StringBuffer buf = new StringBuffer("Recall\tPrecision\tThreshold\n");
	        int skipMod = 1;
	        if (approxDesiredPoints > 0 && allPositive > approxDesiredPoints) {
	            skipMod = (int)(allPositive / approxDesiredPoints);
	        }
	        double cummulativeCorrect = 0;
	        for (int i = 0; i < instances.size(); ++i) {
	            Instance p = instances.get(i);
	            if (p.score == Double.NEGATIVE_INFINITY) 
	                break;
	            if (p.relevant) {
	                ++cummulativeCorrect;
	                double effectiveSeen = (i + 1 - cummulativeCorrect) + cummulativeCorrect;
	                double precision = cummulativeCorrect / effectiveSeen;
	                double recall = cummulativeCorrect / allPositive;
	                if (cummulativeCorrect % skipMod == 0)
	                    buf.append(recall+"\t"+precision+"\t"+p.score+"\n");
	            }
	        }
	        return buf.toString();
	    }
}
