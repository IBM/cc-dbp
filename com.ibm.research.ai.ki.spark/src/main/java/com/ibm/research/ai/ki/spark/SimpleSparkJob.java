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
package com.ibm.research.ai.ki.spark;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.spark.*;
import org.apache.spark.api.java.*;

import com.ibm.research.ai.ki.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public abstract class SimpleSparkJob implements Serializable {
	private static final long serialVersionUID = 1L;

	protected abstract JavaRDD<String> process(JavaRDD<String> documents) throws Exception;

	protected static FileSystem fs;
	
	public void run(String appname, String inputfilename, String outputfilename) {
	    if (appname == null)
	        appname = this.getClass().getSimpleName();
		try (JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(appname))) {
			sc.setLogLevel("ERROR");
			Logger.getRootLogger().setLevel(Level.ERROR);
			long startTime = System.currentTimeMillis();
			
			//check that outpuptfilename does not exist
			fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration());
			if (outputfilename != null && fs.exists(new org.apache.hadoop.fs.Path(outputfilename))) {
				throw new IllegalArgumentException("File already exists: "+outputfilename);
			}

	        //load input, process, and save output
	        JavaRDD<String> input = sc.textFile(inputfilename);
	        JavaRDD<String> output = process(input);
	        if (output != null && outputfilename != null) {
	        	output.saveAsTextFile(outputfilename);
	        }
	        System.out.println("Finished after "+Lang.milliStr(System.currentTimeMillis()-startTime));
		} catch (Exception e) {
			Lang.error(e);
		}
	}
}
