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

public class BlockingThreadedExecutor implements ISimpleExecutor {
	private ThreadPoolExecutor threadPool;
	private int numProcs;
	private int queueDepth;
	
	public int getNumProcessors() {
		return numProcs;
	}
	
	private void buildThreadPool() {
		threadPool = new ThreadPoolExecutor(numProcs - 1, numProcs - 1, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(queueDepth*(numProcs - 1)),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable runnable) {
						Thread thread = Executors.defaultThreadFactory().newThread(runnable);
						thread.setDaemon(true);
						return thread;
					}
				}, new ThreadPoolExecutor.CallerRunsPolicy());		
	}
	
	public BlockingThreadedExecutor(int queueDepth) {
		this.queueDepth = queueDepth;
		
		//executor service, numProcessors-1 == max size == max queue length, caller runs policy
		numProcs = Math.max(2, Runtime.getRuntime().availableProcessors());
		buildThreadPool();
	}
	
	public BlockingThreadedExecutor(int queueDepth, int numThreads) {
		this.queueDepth = queueDepth;
		
		//executor service, numProcessors-1 == max size == max queue length, caller runs policy
		numProcs = Math.max(2, numThreads);
		buildThreadPool();
	}
	
	public BlockingThreadedExecutor() {
		this(1);
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
		awaitFinishing(100);
	}
	
	public void execute(Runnable task) {
		threadPool.execute(task);
	}
	
	public void shutdown() {	
		threadPool.shutdown();
		try {while (!threadPool.awaitTermination(100, TimeUnit.DAYS)); } catch (Exception e) {throw new Error(e);}
	}
}
