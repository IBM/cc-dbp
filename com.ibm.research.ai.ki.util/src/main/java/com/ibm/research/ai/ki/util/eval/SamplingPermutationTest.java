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
/*
Copyright (c) 2012 IBM Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.ibm.research.ai.ki.util.eval;

import java.util.*;
import java.util.function.*;

import com.ibm.research.ai.ki.util.*;

public class SamplingPermutationTest {
	public static class SigResult {
		public double oneTailP;
		public double twoTailP;
		public String toString() {
			return "p = "+twoTailP;
		}
	}

	public static class InstanceIsCorrect implements Function<Collection<Boolean>, Double> {
		public Double apply(Collection<Boolean> insts) {
			double sum = 0;
			for (Boolean d : insts)
				if (d)
					sum += 1.0;
			return sum / insts.size();
		}
	}
	
	public static class AverageScorePerInstance implements Function<Collection<Double>, Double> {
		public Double apply(Collection<Double> insts) {
			double sum = 0;
			for (Double d : insts)
				sum += d;
			return sum / insts.size();
		}
	}
	
	public static class AccuracyMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		double threshold;
		public AccuracyMap(double threshold) {
			this.threshold = threshold;
		}
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			double correct = 0;
			
			for (Pair<Boolean,Double> i : insts) {
				if (i.first == i.second >= threshold) {
					++correct;
				}
			}
			return correct / insts.size();
		}
	}

	public static class MaxFScoreScoreThresholdMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			ArrayList<Pair<Boolean,Double>> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Pair<Boolean,Double>>)insts;
			} else {
				pr = new ArrayList<Pair<Boolean,Double>>(insts);
			}
			SecondPairComparator.sortR(pr);
			double cummulativeTP = 0;
			double allJust = 0;
			for (Pair<Boolean, Double> p : pr) {
				if (p.first)
					allJust += 1;
			}
			double maxF = 0;
			double thresh = 0;
			
			for (int i = 0; i < pr.size(); ++i) {		
				if (pr.get(i).first) 
					++cummulativeTP;
				double tp = cummulativeTP;
				double fp = (i+1) - cummulativeTP;
				double fn = allJust - cummulativeTP;
				
				double precision = (double)(tp)/(tp+fp);
				double recall = (double)(tp)/(tp+fn);
				double f1 = 2*precision*recall/(precision+recall);
				if (f1 > maxF) {
					thresh = pr.get(i).second;
					maxF = f1;
				}
			}
			return thresh;
		}
	}
	
	public static class FScoreScoreThresholdMap implements Function<Collection<Pair<Boolean, Double>>, Double> {
		double threshold;

		public FScoreScoreThresholdMap(double threshold) {
			this.threshold = threshold;
		}

		public Double apply(Collection<Pair<Boolean, Double>> insts) {
			double tp = 0, fp = 0, fn = 0;
			for (Pair<Boolean, Double> inst : insts) {
				if (inst.first) {
					if (inst.second > this.threshold)
						tp++;
					else
						fn++;
				} else if (inst.second > threshold)
					fp++;
			}
			double precision = (double) (tp) / (tp + fp);
			double recall = (double) (tp) / (tp + fn);
			double f1 = 2 * precision * recall / (precision + recall);
			return f1;
		}
	}
	
	public static class PrecisionAt implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public PrecisionAt(int atRank) {
			this.atRank = atRank;
		}
		int atRank;
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			ArrayList<Pair<Boolean,Double>> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Pair<Boolean,Double>>)insts;
			} else {
				pr = new ArrayList<Pair<Boolean,Double>>(insts);
			}
			SecondPairComparator.sortR(pr);
			double correct = 0;
			double count = 0;
			for (int i = 0; i < pr.size() && i < atRank; ++i) {
				if (pr.get(i).first)
					correct += 1;
				count += 1;
			}
			return correct / count;
		}
	}
	
	public static class MaxFScoreScorerMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			ArrayList<Pair<Boolean,Double>> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Pair<Boolean,Double>>)insts;
			} else {
				pr = new ArrayList<Pair<Boolean,Double>>(insts);
			}
			SecondPairComparator.sortR(pr);
			double cummulativeTP = 0;
			double allJust = 0;
			for (Pair<Boolean, Double> p : pr) {
				if (p.first)
					allJust += 1;
			}
			double maxF = 0;
			
			for (int i = 0; i < pr.size(); ++i) {
				if (pr.get(i).second == Double.NEGATIVE_INFINITY)
					break;
				if (pr.get(i).first) 
					++cummulativeTP;
				double tp = cummulativeTP;
				double fp = (i+1) - cummulativeTP;
				double fn = allJust - cummulativeTP;
				
				double precision = (double)(tp)/(tp+fp);
				double recall = (double)(tp)/(tp+fn);
				double f1 = 2*precision*recall/(precision+recall);
				if (f1 > maxF)
					maxF = f1;
			}
			return maxF;
		}
	}
	
	public static class AUCScorerMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			ArrayList<Pair<Boolean,Double>> pr = null;
			if (insts instanceof ArrayList) {
				pr = (ArrayList<Pair<Boolean,Double>>)insts;
			} else {
				pr = new ArrayList<Pair<Boolean,Double>>(insts);
			}
			SecondPairComparator.sortR(pr);
			double cummulativeTP = 0;
			double auc = 0;
			for (int i = 0; i < pr.size(); ++i) {
				if (pr.get(i).second == Double.NEGATIVE_INFINITY)
					break;
				if (pr.get(i).first) {
					++cummulativeTP;
					auc += cummulativeTP/(i+1);
				}
			}
			if (cummulativeTP == 0)
				return 0.0;
			return auc/cummulativeTP;
		}
	}
	public static class PearsonMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			double[] ground = new double[insts.size()];
			double[] prob = new double[insts.size()];
			int ndx = 0; 
			for (Pair<Boolean,Double> p :insts) {
				ground[ndx] = p.first ? 1 : -1;
				prob[ndx] = p.second;
				++ndx;
			}
			return DenseVectors.pearsonsR(ground,prob);
		}
	}


	public static class LogLikeMap implements Function<Collection<Pair<Boolean,Double>>, Double> {
		public Double apply(Collection<Pair<Boolean,Double>> insts) {
			double logLike = 0;
			for (Pair<Boolean,Double> i : insts) {
				double prob = i.second;
				if (prob > 0.99) {
					prob = 0.99;
				}
				if (prob < 0.01) {
					prob = 0.01;
				}
				if (i.first) {
					logLike += Math.log(prob);
				} else {
					logLike += Math.log(1-prob);
				}
			}
			return logLike;
		}
	}
	
	private static <K extends Comparable,T> ArrayList<T> toSortedList(Map<K,T> idToScore) {
		ArrayList<T> list = new ArrayList<T>(idToScore.size());
		ArrayList<Pair<K,T>> plist = HashMapUtil.toPairs(idToScore);
		Collections.sort(plist, new FirstPairComparator());
		for (Pair<K,T> p : plist) {
			list.add(p.second);
		}
		return list;
	}
	
	/**
	 * To compare experiments over the same set of data (each instance is identified by a String id)
	 * @param numSamples
	 * @param baseline
	 * @param experiment
	 * @param scorer
	 * @return
	 */
	public static <K extends Comparable,T> SigResult significance(int numSamples, Map<K,T> baseline, Map<K,T> experiment, Function<Collection<T>,Double> scorer) {
		if (baseline.size() != experiment.size() || !baseline.keySet().containsAll(experiment.keySet())) {
			HashSet<K> bl = new HashSet<K>(baseline.keySet());
			bl.removeAll(experiment.keySet());
			System.err.println("In baseline but not experiment: \n"+Lang.stringList(bl, "\n"));
			HashSet<K> exp = new HashSet<K>(experiment.keySet());
			exp.removeAll(baseline.keySet());
			System.err.println("In experiment but not baseline: \n"+Lang.stringList(exp, "\n"));
			throw new IllegalArgumentException("baseline and experiment must be over the same instances!");
		}
		double baseScore = scorer.apply(baseline.values());
		double expScore = scorer.apply(experiment.values());
		ArrayList<T> baselineList = toSortedList(baseline);
		ArrayList<T> experimentList = toSortedList(experiment);		
		double diff = expScore - baseScore;
		double absDiff = Math.abs(expScore - baseScore);
		int geqDiff = 0;
		int geqAbsDiff = 0;
		Random rand = new Random();
		ArrayList<T> partOne = new ArrayList<T>(baseline.size());
		ArrayList<T> partTwo = new ArrayList<T>(baseline.size());
		for (int i = 0; i < baseline.size(); ++i) {
			partOne.add(null);
			partTwo.add(null);
		}
			
		for (int si = 0; si < numSamples; ++si) {
			for (int ii = 0; ii < baselineList.size(); ++ii) {
				if (rand.nextBoolean()) {
					partOne.set(ii, baselineList.get(ii));
					partTwo.set(ii, experimentList.get(ii));
				} else {
					partTwo.set(ii, baselineList.get(ii));
					partOne.set(ii, experimentList.get(ii));
				}
			}
			double p1Score = scorer.apply(partOne);
			double p2Score = scorer.apply(partTwo);
			if (p2Score - p1Score >= diff) {
				++geqDiff;
			}
			if (Math.abs(p2Score - p1Score) >= absDiff) {
				++geqAbsDiff;
			}
		}
		
		SigResult r = new SigResult();
		r.oneTailP = ((double)geqDiff)/numSamples;
		r.twoTailP = ((double)geqAbsDiff)/numSamples;
		return r;		
	}
	
	/**
	 * To compare experiments that are over different sets of data
	 * @param numSamples
	 * @param baseline
	 * @param experiment
	 * @param scorer
	 * @return
	 */
	public static <T> SigResult significance(int numSamples, Collection<T> baseline, Collection<T> experiment, Function<Collection<T>,Double> scorer) {
		double baseScore = scorer.apply(baseline);
		double expScore = scorer.apply(experiment);
		double diff = expScore - baseScore;
		double absDiff = Math.abs(expScore - baseScore);
		int geqDiff = 0;
		int geqAbsDiff = 0;
		ArrayList<T> allInstances = new ArrayList<T>();
		allInstances.addAll(baseline);
		allInstances.addAll(experiment);
		ArrayList<T> partOne = new ArrayList<T>(baseline);
		ArrayList<T> partTwo = new ArrayList<T>(experiment);
		for (int si = 0; si < numSamples; ++si) {
			Collections.shuffle(allInstances);
			for (int pi = 0; pi < partOne.size(); ++pi) {
				partOne.set(pi, allInstances.get(pi));
			}
			for (int pi = 0; pi < partTwo.size(); ++pi) {
				partTwo.set(pi, allInstances.get(pi+partOne.size()));
			}
			double p1Score = scorer.apply(partOne);
			double p2Score = scorer.apply(partTwo);
			if (p2Score - p1Score >= diff) {
				++geqDiff;
			}
			if (Math.abs(p2Score - p1Score) >= absDiff) {
				++geqAbsDiff;
			}
		}
		SigResult r = new SigResult();
		r.oneTailP = ((double)geqDiff)/numSamples;
		r.twoTailP = ((double)geqAbsDiff)/numSamples;
		return r;
	}
}
