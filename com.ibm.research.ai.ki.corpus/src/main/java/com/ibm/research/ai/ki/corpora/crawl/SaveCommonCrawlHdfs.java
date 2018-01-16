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
package com.ibm.research.ai.ki.corpora.crawl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.*;

import com.ibm.reseach.ai.ki.nlp.*;
import com.ibm.reseach.ai.ki.nlp.types.*;
import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.FileUtil;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.jwat.common.*;
import org.jwat.warc.*;


/**
 * See the common-crawl blog for updates and new crawls: http://commoncrawl.org/connect/blog/
 * For example:
 * http://commoncrawl.org/2017/07/july-2017-crawl-archive-now-available/
 *
 * Saves the English non-boilerplate text from webpages into hdfs.
 * @author mrglass
 *
 */
public class SaveCommonCrawlHdfs extends SaveCommonCrawlBase {
    protected FileSystem fs;
    
    public SaveCommonCrawlHdfs(CommonCrawlConfig config, String hdfsBase) {
        super(config);
        
        try {
            Configuration conf = new Configuration();
            if (!hdfsBase.endsWith("/"))
                hdfsBase = hdfsBase + "/";
            conf.set("fs.defaultFS", hdfsBase);
            conf.set("fs.hdfs.impl", 
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
                );
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
                );
            fs = FileSystem.get(conf);
            System.out.println("Using FileSystem at "+hdfsBase); 
        } catch (Exception e) {
            Lang.error(e);
        }
    }

    @Override
    protected PrintStream getThreadOutput(String baseDir, int threadNum) {
        try {
            OutputStream os = fs.create(new Path(baseDir, "part-"+threadNum));
            return new PrintStream(new BufferedOutputStream(os));
        } catch (Exception e) {
            return Lang.error(e);
        }
    }    
    
    
    /**
     * Example args:
       -hdfsBase hdfs://hostname:8020 
       -urlList /somewhere/june2017-warc.paths 
       -logDir /somewhere/june2017-logs 
       -out /common-crawl/june2017-docs.json.gz.b64
       -config cc-dbp/cc-dbp.properties
       OR
       -hdfsBase hdfs://hostname:8020 
       -urlList https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2017-43/warc.paths.gz 
       -logDir /somewhere/june2017-logs 
       -out /common-crawl/oct2017-docs.json.gz.b64
       -config cc-dbp/cc-dbp.properties
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("config", true, "A RelexConfig in properties file format");   
        options.addOption("urlList", true, "List of WARC urls, see http://commoncrawl.org/connect/blog/");
        options.addOption("out", true, "The HDFS directory to create the dataset in");
        options.addOption("logDir", true, "A directory to save log files in.");
        options.addOption("hdfsBase", true, "Base for HDFS file system.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);  
        } catch (ParseException pe) {
            Lang.error(pe);
        }
        
        String configProperties = cmd.getOptionValue("config");
        String warcUrlList = cmd.getOptionValue("urlList");
        String targetHdfs = cmd.getOptionValue("out");
        String hdfsBase = cmd.getOptionValue("logDir");
        String logDir = cmd.getOptionValue("hdfsBase");
        
        CommonCrawlConfig config = new CommonCrawlConfig();
        config.fromProperties(PropertyLoader.loadProperties(configProperties));
        SaveCommonCrawlHdfs scc = new SaveCommonCrawlHdfs(config, hdfsBase);
        scc.warcDir = logDir;
        scc.saveFormat = DocumentSerialize.formatFromName(targetHdfs);
        Iterable<String> warcUrls = FileUtil.getLines(warcUrlList);

        scc.writeCrawl(warcUrls, targetHdfs);
    }


}
