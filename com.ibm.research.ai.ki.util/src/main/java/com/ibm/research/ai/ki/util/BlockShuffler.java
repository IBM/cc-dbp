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
import java.util.concurrent.*;

/**
 * Shuffles each chunk (block) of the data. Then chunks will be in the same order, but items within each chunk are randomized.
 * @author mrglass
 *
 * @param <T>
 */
public class BlockShuffler<T> implements Iterable<T> {
	public BlockShuffler(Iterable<T> dataset, int blocksize) {
		this.dataset = dataset;
		this.blocksize = blocksize;
		this.rand = new Random();
	}
	public BlockShuffler(Iterable<T> dataset, int blocksize, Random rand) {
		this.dataset = dataset;
		this.blocksize = blocksize;
		this.rand = rand;
	}
	
	Iterable<T> dataset;
	int blocksize;
	Random rand;
	
	class ShufflerThread extends Thread {
		ShufflerThread(Iterator<T> datait) {
			this.datait = datait;
			items = new ArrayBlockingQueue<>(blocksize);
		}
		Iterator<T> datait;
		BlockingQueue<T> items;	
		boolean finished = false;
		
		@Override
		public void run() {
			ArrayList<T> toshuffle = new ArrayList<T>(blocksize);
			
			while (datait.hasNext()) {
				toshuffle.clear();
				for (int i = 0; i < blocksize && datait.hasNext(); ++i) {
					toshuffle.add(datait.next());
				}
				Collections.shuffle(toshuffle, rand);
				for (T s : toshuffle)
					try {
						items.put(s);
					} catch (InterruptedException e) {
						throw new Error(e);
					}
				//System.out.println("Shuffled and added");
			}
			finished = true;
		}
	}
	
	@Override
	public Iterator<T> iterator() {
		ShufflerThread t = new ShufflerThread(dataset.iterator());
		t.setDaemon(true);
		t.start();
		return new Iterator<T>() {
			@Override
			public boolean hasNext() {
				return !(t.items.isEmpty() && t.finished);
			}

			@Override
			public T next() {
				try {
					return t.items.take();
				} catch (InterruptedException e) {
					throw new Error(e);
				}
			}
			
		};
	}

}
