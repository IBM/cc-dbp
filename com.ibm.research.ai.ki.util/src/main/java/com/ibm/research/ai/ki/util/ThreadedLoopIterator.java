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

import java.util.concurrent.*;


public abstract class ThreadedLoopIterator<T> extends NextOnlyIterator<T> {

	private final BlockingQueue<Holder<T>> items;
	private final LoopThread t;
	private boolean tRunning;
	
	//needed because the BlockingQueues can't hold nulls
	private static class Holder<T> {
		Holder(T obj) {
			this.obj = obj;
		}
		T obj;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Holder END = new Holder(null); //marker for end of iterator
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Holder ERROR = new Holder(null); //marker for error
	private volatile Throwable error = new Error("unset");
	
	public ThreadedLoopIterator() {
		this(10);
	}
	public ThreadedLoopIterator(int queueSize) {
		if (queueSize < 1) throw new IllegalArgumentException("queueSize must be positive");
		items = new ArrayBlockingQueue<Holder<T>>(queueSize);
		t = new LoopThread();
		//t.setName("TLI");
		String[] stack = Lang.stackString(new Exception()).split("\n");
		if (stack.length > 2)
		    t.setName("TLI-("+stack[2].substring(stack[2].indexOf('(')+1)); //the line that constructed the ThreadedLoopIterator
		t.setDaemon(true);
	}

	@Override
	public T getNext() {
		if (!tRunning) {
			t.start();
			tRunning = true;
		}
		try {
			Holder<T> h = items.take();
			if (h == ERROR) {
				synchronized (ERROR) {
					Lang.error(error);
				}
			}
			return h.obj;
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	protected class LoopThread extends Thread {	
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			try {
				loop();
				items.put(END);
			} catch (Throwable t) {
				synchronized (ERROR) {
					error = t;
					try {items.put(ERROR);}catch (Throwable ign){}
				}			
			}
		}
	}
	
	/**
	 * loop just goes through and puts each thing using add
	 */
	protected abstract void loop();
	
	protected void add(T item) {
		try {items.put(new Holder<T>(item));}catch (Exception e) {throw new Error(e);}
	}
}
