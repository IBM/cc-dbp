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
package com.ibm.research.ai.ki.util.parallel;

import java.util.concurrent.*;

public class PollingThreadedExecutor implements ISimpleExecutor {
	public ThreadPoolExecutor threadPool;
	private int numProcs;
	private int queueDepth;
	private String name;
	
	public int pollDelayMilli = 100;
	
	public int getNumProcessors() {
		return numProcs;
	}
	
	private void buildThreadPool() {
		BlockingQueue<Runnable> q = queueDepth == Integer.MAX_VALUE ? 
				new LinkedBlockingQueue<>() : 
				new ArrayBlockingQueue<Runnable>(queueDepth*numProcs);
		threadPool = new ThreadPoolExecutor(numProcs, numProcs, 10, TimeUnit.MINUTES, q,
				new ThreadFactory() {
		            int threadCount = 0;
					@Override
					public Thread newThread(Runnable runnable) {
						Thread thread = Executors.defaultThreadFactory().newThread(runnable);
						thread.setDaemon(true);
						if (name != null)
						    thread.setName(name+"-"+threadCount);
						++threadCount;
						return thread;
					}
				}, new ThreadPoolExecutor.AbortPolicy());
	}
	
	public PollingThreadedExecutor(int queueDepth) {
		this.queueDepth = queueDepth;
		numProcs = Runtime.getRuntime().availableProcessors();
		buildThreadPool();
	}
	
	public PollingThreadedExecutor(String name, int queueDepth, int numThreads) {
		this.queueDepth = queueDepth;
		numProcs = numThreads;
		this.name = name;
		buildThreadPool();
	}

	public boolean isFinished() {
		return (threadPool.getQueue().isEmpty() && threadPool.getActiveCount() == 0);
	}
	
	public void awaitFinishing(long milliPoll) {
		try {
			while (!isFinished()) 
				Thread.sleep(milliPoll);	
		} catch (Exception e) { throw new Error(e);}
	}
	
	public void awaitFinishing() {
		awaitFinishing(pollDelayMilli);
	}
	
	public void execute(Runnable task) {
		while (true) {
			try {
				threadPool.execute(task);
				break;
			} catch (RejectedExecutionException e) {
				try {
					Thread.sleep(pollDelayMilli);
				} catch (InterruptedException e1) {
					throw new Error(e1);
				}
			}
		}
	}
	
	public void shutdown() {	
		threadPool.shutdown();
		try {while (!threadPool.awaitTermination(100, TimeUnit.DAYS)); } catch (Exception e) {throw new Error(e);}
	}

}
