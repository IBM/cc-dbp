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

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;

import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public class RunPipelineSpark extends SimpleSparkJob {
    private static final long serialVersionUID = 1L;

    private static Annotator anno = null;
    
    protected Annotator annoUninitialized;
    protected Properties config;
    protected DocumentSerialize.Format writeFormat;
    
    @Override
    protected JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
        return documents.map(new Function<String,String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(String docStr) throws Exception {
                Document doc = DocumentSerialize.fromString(docStr);
                synchronized (RunPipelineSpark.class) {
                    if (anno == null) {
                        anno = annoUninitialized;
                        anno.initialize(config != null ? config : new Properties());
                    }
                }
                anno.process(doc);
                return DocumentSerialize.toString(doc, writeFormat);
            }
            
        });
    }

    /**
     * Example args:
       /blekko/data/financialDocsLinked.json
       /blekko/data/financialDocsOpenIE.ser.gz.b64
       /disks/diskm/mglass/openiePipeline.ser.gz
     * @param args
     */
    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];
        RunPipelineSpark rps = new RunPipelineSpark();
        rps.annoUninitialized = FileUtil.loadObjectFromFile(args[2]);
        if (args.length > 3) {
            rps.config = PropertyLoader.fromString(FileUtil.readFileAsString(args[3]));
        }
        rps.writeFormat = DocumentSerialize.formatFromName(args[1]);
        
        rps.run("Spark Pipeline "+args[2], input, output);
    }
}
