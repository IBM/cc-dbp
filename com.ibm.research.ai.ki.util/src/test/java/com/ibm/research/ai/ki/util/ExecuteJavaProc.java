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

import java.io.File;
import java.io.IOException;

import com.ibm.research.ai.ki.util.*;

public class ExecuteJavaProc {
  private ExecuteJavaProc() {
  }

  public static int exec(Class klass) throws IOException, InterruptedException {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String className = klass.getCanonicalName();
    ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className);
    Process process = builder.start();
    FileUtil.readProcessAsString(process);
    process.waitFor();
    return process.exitValue();
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    exec(BjUtilTestCounter.class);
  }
}
