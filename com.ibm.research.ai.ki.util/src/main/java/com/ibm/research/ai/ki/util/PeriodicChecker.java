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

package com.ibm.research.ai.ki.util;

import java.util.*;

public class PeriodicChecker {
	double seconds;
	long lastTime;
	long firstTime;
	int checkEvery;
	long checkNumber;
	
	public PeriodicChecker(double seconds) {
		this(seconds, false);
	}
	
	public double getInterval() {
		return seconds;
	}
	
	public PeriodicChecker(double seconds, boolean alwaysCheck) {
		this.seconds = seconds;
		lastTime = System.currentTimeMillis();
		firstTime = lastTime;
		checkEvery = alwaysCheck ? 0 : 1;
		checkNumber = 0;
	}
	
	public static <T> Iterable<T> reportEvery(String message, int seconds, Iterable<T> items) {
		return new Iterable<T>() {
			//CONSIDER: create for each iterator? report count of iterators created?
			PeriodicChecker r = new PeriodicChecker(seconds);
			@Override
			public Iterator<T> iterator() {
				Iterator<T> it = items.iterator();
				return new Iterator<T>() {
					@Override
					public boolean hasNext() {
						return it.hasNext();
					}
					@Override
					public T next() {
						if (r.isTime()) {
							System.out.println(message+" on "+r.checkCount()+" after "+Lang.milliStr(r.elapsedTime()));
						}
						return it.next();
					}
				};
			}	
		};
	}
	
	/**
	 * 
	 * @return time elapsed since construction in milliseconds
	 */
	public long elapsedTime() {
		return System.currentTimeMillis() - firstTime;
	}
	
	/**
	 * number of times isTime() has been called
	 * @return
	 */
	public long checkCount() {
		return checkNumber;
	}
	
	/**
	 * Set the last time to the current time.
	 */
	public void reset() {
		lastTime = System.currentTimeMillis();
	}
	
	/**
	 * Checks if the time specified in the constructor has elapsed, resets clock if it has and returns true
	 * @return
	 */
	public synchronized boolean isTime() {
		if (++checkNumber % checkEvery != 0 && checkEvery > 0) {
			return false;
		}
		long time = System.currentTimeMillis();
		double elapsed = (time - lastTime)/1000.0;
		if (elapsed < seconds/20) {
			checkEvery *= 2;
		}
		if (elapsed < seconds) {
			return false;
		}
		if (elapsed > seconds * 1.5) {
			checkEvery /= 4;
			if (checkEvery == 0) checkEvery = 1;
		}
		lastTime = time;
		//checkNumber = 0;
		return true;
	}
}
