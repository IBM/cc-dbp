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
package com.ibm.reseach.ai.ki.nlp;

import java.io.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

/**
 * similar to how Lucene CharFilter works, but serializable and not streaming
 * @author mrglass
 *
 */
public class OffsetCorrection implements Serializable {
	private static final long serialVersionUID = 1L;

	private static final int startArraySize = 64;
	
	//at offset startingAt[ndx] the offset correction is difference[ndx]
	protected int[] startingAt = new int[startArraySize];
	protected int[] difference = new int[startArraySize];
	protected int otherDocLength = Integer.MAX_VALUE;
	protected int size = 0;
	
	public int correct(int offset) {
		int pos = Arrays.binarySearch(startingAt, 0, size, offset);
		if (pos < 0) {
			pos = -(pos+1)-1;
		}
		int correction = 0;
		if (pos < 0) {
			correction = 0;
		} else {
			correction = difference[pos];
		}
		if (pos < size - 1 && startingAt[pos+1] + difference[pos+1] < offset + correction)
			return Math.min(otherDocLength, startingAt[pos+1] + difference[pos+1]);
		//System.out.println("pos = "+pos+" difference = "+correction);
		return Math.min(otherDocLength, offset + correction);
		
	}
	
	/**
	 * put the span from the transformed offsets into the original offsets
	 * @param span
	 */
	public void correct(Span span) {
		span.start = correct(span.start);
		span.end = correct(span.end-1)+1; 
	}
	
	/**
	 * when building this up, add the next point where offsets change
	 * must be added in sequence
	 * @param startingAtPos
	 * @param cummulativeDifference
	 */
	public void addNextCorrectionPoint(int startingAtPos, int cummulativeDifference) {
		if (size >= startingAt.length) {
			startingAt = Arrays.copyOf(startingAt, size*2);
			difference = Arrays.copyOf(difference, size*2);
		}
		startingAt[size] = startingAtPos;
		difference[size] = cummulativeDifference;
		++size;
	}
	
	/**
	 * to prevent offering corrections outside the bounds of the other document
	 * @param otherDocLength
	 */
	public void setOtherDocLength(int otherDocLength) {
		this.otherDocLength = otherDocLength;
	}
	
	//CONSIDER: may want to merge the corrections from two transformations
	//public void merge(OffsetCorrection oc) {
	//	throw new UnsupportedOperationException();
	//}
}
