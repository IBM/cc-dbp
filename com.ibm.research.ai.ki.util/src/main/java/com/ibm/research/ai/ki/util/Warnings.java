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
import java.util.concurrent.*;
import java.util.logging.*;


public class Warnings {
	//No need to be super careful about multi-threading issues. Doesn't matter if we output slightly too few or too many warnings
	//just don't crash with concurrent mod. exception
	private static Map<String, MutableInteger> warningCount = new ConcurrentHashMap<String, MutableInteger>();
	private static Map<String, FreqCheck> warningLastTime = new ConcurrentHashMap<String, FreqCheck>();
	
	private static class FreqCheck {
		long time;
		int skipCount;
	}
	
	public static boolean limitWarn(Logger log, String category, int limit, String message) {
		MutableInteger catCount = warningCount.get(category);
		if (catCount != null && catCount.value >= limit) {
			return false;
		}
		if (catCount == null) {
			catCount = new MutableInteger(0);
			warningCount.put(category, catCount);
		}		
		catCount.value += 1;
		if (catCount.value >= limit) {
			log.warning(message+" ***(Last warning)***");
		} else {
			log.warning(message);
		}
		
		return true;
	}
	public static boolean limitWarn(String category, int limit, String message) {
		MutableInteger catCount = warningCount.get(category);
		if (catCount != null && catCount.value >= limit) {
			return false;
		}
		if (catCount == null) {
			catCount = new MutableInteger(0);
			warningCount.put(category, catCount);
		}		
		catCount.value += 1;
		if (catCount.value >= limit) {
			System.err.println(message+" ***(Last warning)***");System.err.flush();
		} else {
			System.err.println(message);System.err.flush();
		}
		
		return true;
	}
	
	/**
	 * Issue warning on stderr but limit the frequency to at most once every [seconds] seconds
	 * @param category
	 * @param seconds
	 * @param message
	 * @return true if the message was output
	 */
	public static boolean freqWarn(String category, int seconds, String message) {
		FreqCheck last = warningLastTime.get(category);
		if (last == null) {
			last = new FreqCheck();
			warningLastTime.put(category, last);
		}
		long time = System.currentTimeMillis();
		if (last.time + seconds*1000 > time) {
			++last.skipCount;
			return false;
		}
		last.time = time;
		if (last.skipCount > 0) {
			System.err.println(message+" ***(skipped "+last.skipCount+")***");
		} else {
			System.err.println(message);
		}
		
		last.skipCount = 0;
		return true;
	}
	
	public static void reset() {
		warningCount.clear();
	}
}
