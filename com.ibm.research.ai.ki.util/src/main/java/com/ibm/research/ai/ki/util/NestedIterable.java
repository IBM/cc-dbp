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
import java.util.function.*;

//CONSIDER: make NestedIterable AutoCloseable itself - track iterators and close them
public class NestedIterable<S,T> implements Iterable<T> {
	Iterable<S> outerIterable;
	Function<S,Iterator<T>> outer2Inner;
	
	public NestedIterable(Iterable<S> outer, Function<S,Iterator<T>> outer2Inner) {
		outerIterable = outer;
		this.outer2Inner = outer2Inner;
	}

	@Override
	public Iterator<T> iterator() {
		return new NestedIterator();
	}
	
	class NestedIterator implements Iterator<T>, AutoCloseable {
		NestedIterator() {
			outer = outerIterable.iterator();
			nextInnerIt();
		}
		
		//CONSIDER: static and move to FileUtil?
		private void tryClose(Object it) {
			try {
				if (it instanceof AutoCloseable) {
					((AutoCloseable)it).close();			
				}
			} catch (Throwable t) {
				Warnings.limitWarn("badclose", 3, "tried to close "+it.getClass().getCanonicalName()+" but: "+t.getMessage());
			}
		}
		
		private void nextInnerIt() {
			while (inner == null || !inner.hasNext())
				if (outer.hasNext()) {
					tryClose(inner);
					inner = outer2Inner.apply(outer.next());
				} else {
					tryClose(inner);
					inner = null;
					return;
				}
		}
		
		Iterator<S> outer;
		Iterator<T> inner;
		@Override
		public boolean hasNext() {
			return inner != null && inner.hasNext();
		}

		@Override
		public T next() {
			T ret = inner.next();
			if (inner != null && !inner.hasNext())
				nextInnerIt();
			return ret;
		}

		@Override
		public void remove() {
			inner.remove();
		}

		@Override
		public void close() throws Exception {
			tryClose(inner);
			tryClose(outer);
		}
		
	}

}
