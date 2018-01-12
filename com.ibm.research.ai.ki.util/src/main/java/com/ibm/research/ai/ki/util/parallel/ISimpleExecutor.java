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

public interface ISimpleExecutor {
	/**
	 * The number of threads that this executor uses.
	 * @return
	 */
	public int getNumProcessors();
	/**
	 * Waits for all tasks to finish, polling every milliPoll milliseconds
	 * @param milliPoll
	 */
	public void awaitFinishing(long milliPoll);
	/**
	 * Waits for all tasks to finish
	 */
	public void awaitFinishing();
	/**
	 * Add the task to the list of things to execute in parallel
	 * @param task
	 */
	public void execute(Runnable task);
	
	/**
	 * Waits for all submitted tasks to finish but no longer accepts additional tasks.
	 * The executor cannot be used after this is executed.
	 */
	public void shutdown();
	
	/**
	 * True if all submitted tasks have finished
	 * @return
	 */
	public boolean isFinished();
}
