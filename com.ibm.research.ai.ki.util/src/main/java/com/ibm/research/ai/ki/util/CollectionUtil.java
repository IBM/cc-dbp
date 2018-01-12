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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CollectionUtil {

  /**
   * Given a sequence, partitions it into roughly numFolds equal sized folds. Some folds will
   * contain one less item than others if the number of items do not divide evenly.
   */
	@SuppressWarnings("unchecked")
	public static <T> List<T>[] partition(Iterable<T> seq, int numPartitions) {
		if (numPartitions < 1)
			throw new IllegalArgumentException("numPartitions must be positive");
		if (numPartitions == 1) {
			if (seq instanceof List)
				return new List[] { (List<T>) seq };
			List<T>[] ret = new List[] { new ArrayList<T>() };
			for (T s : seq)
				ret[0].add(s);
			return ret;
		}
		List<T>[] folds = new List[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			folds[i] = new ArrayList<T>();
		}

		int index = 0;
		for (T element : seq) {
			folds[index % numPartitions].add(element);
			index++;
		}
		return folds;
	}

  /**
   * This helps you convert a collection of items into a series of train/test divisions for the
   * purpose of running cross-validation or jack-knifing.
   * 
   * First, this splits the data into numDivisions folds. Given a sequence of items, produces a list
   * of divisions. Each division contains a pair of collections which represent the training and
   * testing set. Each fold of the data will be used as the test set in only one division.
   */
  public static <T> List<Pair<Collection<T>, Collection<T>>> partitionToCrossvalidatedTrainTestDivisions(
          Collection<T> seq, int numDivisions) {
    List<Pair<Collection<T>, Collection<T>>> divisions = new ArrayList<Pair<Collection<T>, Collection<T>>>(numDivisions);
    List<T>[] folds = partition(seq, numDivisions);

    for (int i = 0; i < numDivisions; i++) {
      Collection<T> test = folds[i];
      Collection<T> train = new ArrayList<T>();
      for (int j = 0; j < numDivisions; j++) {
        if (i == j) {
          continue;
        }
        train.addAll(folds[j]);
      }
      divisions.add(Pair.of(train, test));
    }

    return divisions;
  }
}
