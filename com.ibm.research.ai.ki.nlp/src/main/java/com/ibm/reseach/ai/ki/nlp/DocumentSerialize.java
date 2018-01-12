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
package com.ibm.reseach.ai.ki.nlp;

import java.io.*;
import java.nio.charset.*;
import java.util.*;
import java.util.zip.*;

import com.ibm.research.ai.ki.util.*;
import com.ibm.research.ai.ki.util.io.*;
/**
 * Handles serializing Documents either with java serialization or json, with and without compression.
 * @author mrglass
 *
 */
public class DocumentSerialize {
    public enum Format {
        json,
        json_gz_b64,
        ser_gz_b64
    }
    
    public static Class<? extends Annotation> annotationClassName(String name) {
        try {
            Class<? extends Annotation> c = DocumentJSONDeserializer.forName(name);
            if (!Annotation.class.isAssignableFrom(c))
                return null;
            return c;
        } catch (Throwable t) {
            return null;
        }
    }
    
    public static Format formatFromName(String name) {
        if (name.endsWith("json")) {
            return Format.json;
        } else if (name.endsWith("json.gz.b64")) {
            return Format.json_gz_b64;
        } else if (name.endsWith("ser.gz.b64")) {
            return Format.ser_gz_b64;
        } else {
            throw new UnsupportedOperationException(name +" extension not recognized.");
        }
    }
    
    public static String toString(Document doc, Format format) {
        if (format == Format.json) {
            return DocumentJSONSerializer.toJSON(doc);
        } else if (format == Format.json_gz_b64) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                OutputStreamWriter os = new OutputStreamWriter(new GZIPOutputStream(baos));
                os.write(DocumentJSONSerializer.toJSON(doc));
                os.flush(); os.close();
                return Base64.getEncoder().encodeToString(baos.toByteArray());
            } catch (Exception e) {
                return Lang.error(e);
            }
        } else if (format == Format.ser_gz_b64) {
            return FileUtil.objectToBase64String(doc);
        } else {
            throw new UnsupportedOperationException("Not implemented: "+format);
        }
    }
    
    public static Document fromString(String str) {
        try {
            //check if it is json
            if (str.charAt(0) == '{')
                return DocumentJSONDeserializer.fromJSON(str);
            
            //if not json it is base64 encoded somehow
            byte[] decodedBytes = Base64.getDecoder().decode(str);
            
            //uncompress if it is compressed
            decodedBytes = FileUtil.uncompress(decodedBytes);

            //check if it is java serialized object
            if (decodedBytes.length > 2 && decodedBytes[0] == (byte)0xAC && decodedBytes[1] == (byte)0xED) {
                ObjectInputStream ois = new RefactoringObjectInputStream(new ByteArrayInputStream(decodedBytes));
                Document object = (Document) ois.readObject();
                ois.close();
                return object;
            }
            
            //must have been compressed json
            return DocumentJSONDeserializer.fromJSON(new String(decodedBytes, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return Lang.error(e);
        }
    }
    
    /**
     * Convert document collection format to json, one Document per line
     * Example args:
       wikinewsDocs wikinewsDocs.json
     * @param args
     */
    public static void main(String[] args) {
        try (PrintStream out = FileUtil.getFilePrintStream(args[1])) {
            for (Document doc : new DocumentReader(new File(args[0]))) {
                out.println(DocumentJSONSerializer.toJSON(doc));
            }
        }
    }
}
