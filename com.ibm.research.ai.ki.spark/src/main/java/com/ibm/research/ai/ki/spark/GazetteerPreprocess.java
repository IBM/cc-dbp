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
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.kbp.*;
import com.ibm.research.ai.ki.nlp.parse.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public class GazetteerPreprocess extends SimpleSparkJob {
    private static final long serialVersionUID = 1L;
    
    protected DocumentSerialize.Format format;
    protected GazetteerEDL edl;
    
    public GazetteerPreprocess(GazetteerEDL edl, DocumentSerialize.Format format) {
        this.edl = edl;
        this.format = format;
    }
    

    @Override
    protected JavaRDD<String> process(JavaRDD<String> documents) throws Exception {
        return documents.map(new Function<String,String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String dstr) throws Exception {
                return DocumentSerialize.toString(edl.annotate(dstr), format);
            }
            
        });
    }

    
    public static void main(String[] args) throws Exception {
        String inputCorpus = args[0];
        String outputCorpus = args[1];
        String gazFile = args[2];
        
        DocumentSerialize.Format format = DocumentSerialize.formatFromName(outputCorpus);
        GazetteerPreprocess gp = new GazetteerPreprocess(new GazetteerEDL(gazFile), format);
        gp.run("Gazetteer Preprocessing on "+inputCorpus, inputCorpus, outputCorpus);
    }
}
