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
package com.ibm.research.ai.ki.util.io;

/**
 * Used by RefactoringObjectInputStream
 * When breaking serialization compatibility for a class Foo
 * 1) copy the old version to a new class name FooV1.
 * 2) have FooV1 implement OldVersionOf Foo
 * 3) change Foo in the way desired and write the convert function for FooV1
 * 4) update the serialVersionId in Foo
 * 5) create a mapping in serializedMappings.properties: com.ibm.Foo:oldSerialVersionId -> com.ibm.FooV1
 * @author mrglass
 *
 * @param <T> the class that it is an old version of
 */
public interface OldVersionOf<T> {
	public T convert();
}
