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

public class LogLinear {
	/**
	 * inverse of logistic, log-odds
	 * @param x
	 * @return
	 */
	public static double logit(double x) {
		return Math.log(x / (1-x));
	}
	/**
	 * Sigmoid function
	 * @param x
	 * @return
	 */
	public static double logistic(double x) {
		return 1.0/(1.0+Math.exp(-x));
	}
	/**
	 * smoothes x away from values too close to zero or one
	 * x will be in the range [smoothby, (1-smoothby)] if it was in the range [0,1] originally
	 * @param x
	 * @param smoothby
	 * @return
	 */
	public static double smooth(double x, double smoothby) {
		return (1-2*smoothby)*x+smoothby;
	}
}
