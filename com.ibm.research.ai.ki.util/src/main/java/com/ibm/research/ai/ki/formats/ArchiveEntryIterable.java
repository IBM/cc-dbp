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
package com.ibm.research.ai.ki.formats;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import com.ibm.research.ai.ki.util.*;

import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.bzip2.*;
import org.apache.commons.compress.compressors.gzip.*;

/**
 * Iterate over text files in a zip or tar or tgz archive.
 * Entrys have name and content.
 * 
 * @author mrglass
 *
 */
public class ArchiveEntryIterable implements Iterable<ArchiveEntryIterable.Entry> {

    public ArchiveEntryIterable(File f) {
        archiveFile = f;
    }
    
    public static class Entry {
        public String name;
        public String content;
        
        public Entry(String name, String content) {
            this.name = name;
            this.content = content;
        }
    }
    
    protected File archiveFile;
    
    static final int BUFFER_SIZE = 4096;
    public static String readEntryAsString(TarArchiveInputStream tarIn) throws Exception {
        int count;
        byte data[] = new byte[BUFFER_SIZE];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((count = tarIn.read(data, 0, BUFFER_SIZE)) != -1) {
            bos.write(data, 0, count);
        }
        bos.close();
        return bos.toString("UTF-8");
    }
    
    @Override
    public Iterator<Entry> iterator() {
        return new ThreadedLoopIterator<Entry>() {
            protected void tarLoop() throws Exception {
                InputStream is = new FileInputStream(archiveFile);
                if (archiveFile.getName().endsWith(".gz") || archiveFile.getName().endsWith(".tgz")) {
                    is = new GzipCompressorInputStream(is);
                } else if (archiveFile.getName().endsWith("bz2")) {
                    BufferedInputStream bis = new BufferedInputStream(is, BUFFER_SIZE);
                    is = new BZip2CompressorInputStream(bis);
                }
                try (TarArchiveInputStream tarIn = new TarArchiveInputStream(is)) {
                    TarArchiveEntry entry;
                    while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                        if (entry.isDirectory()) {
                            continue;
                        } else {
                            String name = entry.getName();
                            String content = readEntryAsString(tarIn);
                            super.add(new Entry(name, content));
                        }
                    }
                }
            }
            
            protected void zipLoop() throws Exception {
                try (ZipFile zip = new ZipFile(archiveFile)) {
                    Enumeration<? extends ZipEntry> zipIn = zip.entries();
    
                    while (zipIn.hasMoreElements()) {
                        ZipEntry entry = zipIn.nextElement();
                        String name = entry.getName();
                        String content = FileUtil.readStreamAsString(zip.getInputStream(entry));
                        super.add(new Entry(name, content));
                    }
                }
            }
            
            @Override
            protected void loop() {
                try {
                    if (archiveFile.getName().endsWith(".zip") || archiveFile.getName().endsWith(".jar")) {
                        zipLoop();
                    } else if (archiveFile.getName().endsWith(".tgz") || archiveFile.getName().endsWith(".tar") || archiveFile.getName().endsWith(".tar.gz")) {
                        tarLoop();
                    } else {
                        throw new IllegalArgumentException("Could not determine type of archive: "+archiveFile.getName()+" not a .zip, .jar, .tgz, .tar or .tar.gz");
                    }
                } catch (Exception e) {
                    Lang.error(e);
                }
            }         
        };
    }

}
