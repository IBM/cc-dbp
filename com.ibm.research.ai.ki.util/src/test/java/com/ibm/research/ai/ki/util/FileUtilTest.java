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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

import com.ibm.research.ai.ki.util.*;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUtilTest extends TestCase {
  @Rule
  public TemporaryFolder folder;
  private File testDir = null; 
  private String childDirA = null;
  private String childDirB =  null;
  private String childDirC =  null;
  private String childDirD =  null;
  private String fileName1 =  null;
  private String fileName1Zipped =  null;
  private String fileName2 =  null;
  private String fileName3 =  null;
  private String objectFileName =  null;
  private String objectFileNameZipped =  null;
  private String fileText =  null;

  @Before
  public void setUp() throws Exception {
	folder = new TemporaryFolder();
	folder.create();
    testDir = folder.newFolder("fileTests");
    childDirA = testDir+File.separator+"childDirA";
    childDirB = testDir+File.separator+"childDirB";
    childDirC = childDirA+File.separator+"childDirC";
    childDirD = childDirC+File.separator+"childDirD/";
    fileName1 = childDirA+File.separator+"testFile1.txt";
    fileName1Zipped = fileName1+".gz";
    fileName2 = childDirD+File.separator+"testFile2.txt";
    fileName3 = childDirB+File.separator+".testFile3.txt";
    objectFileName = childDirA+File.separator+"person1.ser";
    objectFileNameZipped = objectFileName+".gz";
    fileText = "Sample Text";
  }

  @After
  public void tearDown() throws Exception {    //folder.delete();
  }

  @Test
  public void testReadAndWriteFileAsStringFileString() {
    FileUtil.writeFileAsString(fileName1, fileText);
    assertEquals(fileText, FileUtil.readFileAsString(fileName1));
    FileUtil.writeFileAsString(fileName1Zipped, fileText);
    assertEquals(fileText, FileUtil.readFileAsString(fileName1Zipped));
  }

  @Test
  public void testEnsureWriteable() {
    FileUtil.ensureWriteable(new File(fileName2));
    assertEquals(true, FileUtil.exists(childDirD));
  }

  @Test
  public void testReadStreamAsString() throws FileNotFoundException {
	FileUtil.writeFileAsString(fileName1, fileText);
    assertEquals(fileText, FileUtil.readStreamAsString(new FileInputStream(new File(fileName1))));
  }

  @Test
  public void testSaveAndLoadObjectAsFileFileObject() {
    FileUtil.saveObjectAsFile(objectFileName, new Person("John", "Doe"));
    assertEquals("John" ,((Person)FileUtil.loadObjectFromFile(objectFileName)).getFirstName());
    FileUtil.saveObjectAsFile(objectFileNameZipped, new Person("John", "Doe"));
    assertEquals("Doe" ,((Person)FileUtil.loadObjectFromFile(objectFileNameZipped)).getLastName());
  }
  
  @Test
  public void testGetExtension() {
    assertEquals(".txt" ,FileUtil.getExtension(fileName1));
    assertEquals(".gz" ,FileUtil.getExtension(fileName1Zipped));
    FileUtil.writeFileAsString(fileName3, "Hidden Text");
    assertEquals("" ,FileUtil.getExtension(childDirB));
    assertEquals(".txt" ,FileUtil.getExtension(fileName3));
  }

  @Test
  public void testRemoveExtension() {
    assertEquals("fileTests/childDirB/.testFile3", FileUtil.removeExtension("fileTests/childDirB/.testFile3"));
  }

  @Test
  public void testEnsureSlash() {
    assertEquals(childDirD ,FileUtil.ensureSlash(childDirD));
    assertEquals(childDirA+"/",FileUtil.ensureSlash(childDirA));
  }

  @Test
  public void testExists() {
	FileUtil.ensureWriteable(new File(childDirD,"x"));
    assertEquals(true, FileUtil.exists(childDirD));
  }
  
  @Test
  public void testGetCanonicalPath() throws IOException {
    File f = new File(fileName1);
    assertEquals(f.getCanonicalPath(),FileUtil.getCanonicalPath(f));
  }
  
  @Test
  public void testFastStream() throws IOException {
	FileUtil.writeFileAsString(fileName1, fileText);
    File f = new File(fileName1);
    assertEquals(fileText,FileUtil.readStreamAsString(FileUtil.fastStream(f)));
  }

  public static void delete(File file) throws IOException {
    if (file.isDirectory()) {
      if (file.list().length == 0) {
        file.delete();
      } else {
        String files[] = file.list();
        for (String temp : files) {
          File fileDelete = new File(file, temp);
          delete(fileDelete);
        }
        if (file.list().length == 0) {
          file.delete();
        }
      }
    } else {
      file.delete();
    }
  }
  
  static class Person implements Serializable{
    private static final long serialVersionUID = -4307571666794029388L;
    private String firstName;
    private String lastName;
    public Person(String firstName, String lastName) {
      this.firstName = firstName;
      this.lastName = lastName;
    }
    public String getFirstName() {
      return firstName;
    }
    public String getLastName() {
      return lastName;
    }
    
    @Override
    public String toString() {
      return getFirstName()+":"+getLastName();
    }
  }

}
