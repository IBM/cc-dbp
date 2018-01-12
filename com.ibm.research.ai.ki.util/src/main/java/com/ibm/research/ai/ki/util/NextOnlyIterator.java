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

import java.util.*;

public abstract class NextOnlyIterator<T> implements Iterator<T>, AutoCloseable {
	private T next;
	private boolean done = false;
	
	abstract protected T getNext();

	
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean hasNext() {
		if (done)
			return false;
		if (next == null)
			next = getNext();
		done = next == null;
		if (done)
			close();
		return !done;
	}

	@Override
	public T next() {
		if (done)
			return null;
		if (next != null) {
			T toRet = next;
			next = null;
			return toRet;
		}
		T toRet = getNext();
		done = toRet == null;
		if (done)
			close();
		return toRet;
	}
	
	public void close() {}
	
	@Override
	protected void finalize() throws Throwable {
	    super.finalize();
	    if (!done)
	    	close();
	}
}
