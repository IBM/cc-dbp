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

import java.io.*;
import java.net.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.*;
import org.apache.commons.compress.compressors.bzip2.*;
import org.apache.commons.compress.utils.*;

import com.ibm.research.ai.ki.util.io.*;
import com.ibm.research.ai.ki.util.parallel.*;


/**
 * When reading from or writing to a file, a filename ending with .gz indicates the file
 * is/should-be gzipped for any write method, if any directories need to be created to write the
 * file, they should be created automatically the methods should not throw checked exceptions, if
 * there is an I/O problem, they throw an Error
 * 
 * @author psuryan
 * @author mrglass
 * 
 * 
 */
public class FileUtil {

  public static String GZIP_EXTN = ".gz";
  public static String BZIP2_EXTN = ".bz2";

  public static String EXTN_CHR = ".";

  private static final int BUFFER_SIZE = 2 << 16;
  
	public static OutputStream getOutputStream(File file) {
		try {
			ensureWriteable(file);
			OutputStream os = new FileOutputStream(file);
			if (file.getName().endsWith(GZIP_EXTN)) {
				return new GZIPOutputStream(os, BUFFER_SIZE);
			} else if (file.getName().endsWith(BZIP2_EXTN)) {
				return new BZip2CompressorOutputStream(os);
			}
			return new BufferedOutputStream(os, BUFFER_SIZE);
		} catch (Exception e) {
			return Lang.error(e);
		}
	}

    private static InputStream getInputStream(String fileOrUrl) throws FileNotFoundException, IOException {
        InputStream is = null;
        if (fileOrUrl.contains("://"))
            is = new URL(fileOrUrl).openStream();
        else
            is = new FileInputStream(new File(fileOrUrl));
        if (fileOrUrl.endsWith(GZIP_EXTN)) {
            return new GZIPInputStream(is, BUFFER_SIZE);
        } else if (fileOrUrl.endsWith(BZIP2_EXTN)) {
            try {
                BufferedInputStream bis = new BufferedInputStream(is, BUFFER_SIZE);
                // this version can handle a number of different formats:
                // CompressorInputStream input = new
                // CompressorStreamFactory().createCompressorInputStream(bis);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
                return bzIn;
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
        }
        return new BufferedInputStream(is, BUFFER_SIZE);
    }
	
  private static InputStream getInputStream(File file) throws FileNotFoundException, IOException {
    InputStream is = new FileInputStream(file);
    if (file.getName().endsWith(GZIP_EXTN)) {
      return new GZIPInputStream(is, BUFFER_SIZE);
    } else if (file.getName().endsWith(BZIP2_EXTN)) {
    	try {
	        BufferedInputStream bis = new BufferedInputStream(is, BUFFER_SIZE);
	        //this version can handle a number of different formats: CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
	        BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
	        return bzIn;
    	} catch (Exception e) {
    		throw new IllegalArgumentException(e);
    	}
    }
    return new BufferedInputStream(is, BUFFER_SIZE);
  }

  private static boolean isDirOrHiddenOrHasNoDot(File f) {
    String name = f.getName();
    if (f.isDirectory()) { // dir
      return true;
    }
    if (!name.contains(EXTN_CHR)) { // no extn file
      return true;
    }
    if (name.startsWith(EXTN_CHR)) { // hidden
      if (name.lastIndexOf(EXTN_CHR) == name.indexOf(EXTN_CHR)) { // no
        // extn
        return true;
      }
    }
    return false;
  }

  /**
   * writes the text to the file wither compressed or uncompressed
   * 
   * @param file
   * @param text
   */
  public static void writeFileAsString(File file, String text) {
    try {
      OutputStream os = getOutputStream(file);
      // since we are not sure about the stream type anymore
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
      out.write(text);
      out.close();
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  /**
   * writes the text to the file, given a fileName and text
   * 
   * @param filename
   * @param text
   */
  public static void writeFileAsString(String filename, String text) {
    File f = new File(filename);
    ensureWriteable(f);
    writeFileAsString(f, text);
  }

  /**
   * creates any directories that need to exist to create and write the file should not create the
   * file
   */
  public static void ensureWriteable(File f) {
    File parent = f.getParentFile();
    if (parent == null) //has no parents, fine
    	return;
    if (!parent.exists() && !parent.mkdirs()) {
      throw new IOError(new IllegalStateException("Couldn't create parent dir: " + parent));
    }
  }

  /**
   * Read the contents of the file into a string and return it.
   * Also suitable for urls.
   * 
   * @param filename
   * @return file contents as String
   */
    public static String readFileAsString(String filename) {
        try {
            return readStreamAsString(getInputStream(filename), "UTF-8");
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            throw new Error(e);
        }
    }

  /**
   * Read the contents of the file into a string and return it if the file doesn't exist, they
   * throw IllegalArgumentException. Based on
   * https://weblogs.java.net/blog/pat/archive/2004/10/stupid_scanner_1.html (APACHE LICENSE)
   * 
   * @param file
   * @return file contents as String
   */
  public static String readFileAsString(File file) {
      if (!file.exists())
          throw new IllegalArgumentException("No such file: "+file.getAbsolutePath());
	  return readFileAsString(file, "UTF-8");
  }
  public static String readFileAsString(File file, String encoding) {
    try {
      return readStreamAsString(getInputStream(file), encoding);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new Error(e);
    }
  }

  public static String readStreamAsString(InputStream is) {
	  return readStreamAsString(is, "UTF-8");
  }
  /**
   * reads the input stream into a String and closes it
   * 
   * @param is
   * @return stream contents as String
   */
  public static String readStreamAsString(InputStream is, String encoding) {
    String content;
    Scanner scanner = new Scanner(is, encoding).useDelimiter("\\A");
    content = scanner.hasNext() ? scanner.next() : "";
    scanner.close();
    return content;
  }

  /**
   * save the presumably Serializable object to the file
   * 
   * @param file
   * @param object
   */
  public static void saveObjectAsFile(File file, Object object) {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(getOutputStream(file));
      oos.writeObject(object);
      oos.flush();
      oos.close();
    } catch (IOException ioe) {
      throw new IOError(ioe);
    }
  }

  /**
   * save the presumably Serializable object to the file, given a fileName
   * 
   * @param file
   * @param object
   */
  public static void saveObjectAsFile(String fileName, Object object) {
    saveObjectAsFile(new File(fileName), object);
  }

  /**
   * assume the file contains exactly one serialized object, read it and return it if the file
   * doesn't exist, they return null
   * 
   * @param file
   * @return object of type T
   */
  @SuppressWarnings("unchecked")
  public static <T> T loadObjectFromFile(File file) {
    try {
      ObjectInputStream ois = new RefactoringObjectInputStream(getInputStream(file));
      T object = (T) ois.readObject();
      ois.close();
      return object;
    } catch (FileNotFoundException fe) {
      return null;
    } catch (IOException ioe) {
      throw new IOError(ioe);
    } catch (ClassNotFoundException cnfe) {
      throw new Error(cnfe);
    }
  }
  
  /**
   * Deserializes the object from a base64 encoded string. The string is assumed to contain exactly one object.
   * @param base64enc
   * @return
   */
  public static <T> T objectFromBase64String(String base64enc) {
	  try {
		  byte[] decodedBytes = Base64.getDecoder().decode(base64enc);
		  ObjectInputStream ois = new RefactoringObjectInputStream(
				  new GZIPInputStream(new ByteArrayInputStream(decodedBytes)));
	      T object = (T) ois.readObject();
	      ois.close();
	      return object;
	  } catch (Exception e) {
		  return Lang.error(e);
	  }
  }
  /**
   * Serializes the object into a base64 encoded string.
   * @param object
   * @return
   */
  public static String objectToBase64String(Object object) {
	  try {
		  ByteArrayOutputStream baos = new ByteArrayOutputStream();
		  ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(baos));
	      oos.writeObject(object);
	      oos.flush();
	      oos.close();
		  return Base64.getEncoder().encodeToString(baos.toByteArray());
	  } catch (Exception e) {
		  return Lang.error(e);
	  }
  }

  /**
   * Given a fileName, assume the file contains exactly one serialized object, read it and return it
   * if the file doesn't exist, they return null
   * 
   * @param file
   * @return object of type T
   */
  public static <T> T loadObjectFromFile(String fileName) {
    return loadObjectFromFile(new File(fileName));
  }
  
  public static class ObjectStreamIterator<T> extends NextOnlyIterator<T> {
	protected ObjectInputStream ois;
	public ObjectStreamIterator(File file) {
		try {
			ois = new RefactoringObjectInputStream(getInputStream(file));
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	public ObjectStreamIterator(InputStream is) {
		try {
			ois = new RefactoringObjectInputStream(is);
		} catch (Exception e) {
			Lang.error(e);
		}
	}
	  
	@Override
	protected T getNext() {
		try {
			return (T)ois.readObject();
		} catch (EOFException eof) {
			return null;
		} catch (Exception e) {
			return Lang.error(e);
		}
	}
	
	public void close() {
		try {
			if (ois != null) ois.close();
			ois = null;
		} catch (Exception e) {}
	}
  }

  /**
   * remove the last extension from a file If it is a directory, then return the name If it is a
   * hidden file without extn, return the name If it is a hidden file with extn, return name sans
   * extn If is a regular file with extn, return the name sans extn
   * 
   * @param fileName
   * @return fileName without extension
   */
  public static String removeExtension(String fileName) {
    File f = new File(fileName);
    int lastDotLoc = fileName.lastIndexOf(EXTN_CHR);
    if (isDirOrHiddenOrHasNoDot(f)) {
      return fileName;
    } else {
      if (lastDotLoc > fileName.indexOf(File.separatorChar)) {
        return fileName.substring(0, lastDotLoc);
      } else {
        return fileName;
      }
    }
  }

	/**
	 * removes the '.whatever.txt.gz' at the end of the file. If the file has multiple extensions it removes them all.
	 * @param file
	 * @return
	 */
	public static String removeExtensions(String file) {
		int extPos = file.indexOf(EXTN_CHR);
		if (extPos != -1 && extPos > file.lastIndexOf(File.separatorChar)) {
			file = file.substring(0, extPos);
		}
		return file;
	}  
  
  /**
   * return the last extension from a filename, including the '.'
   * 
   * @param fileName
   */
  public static String getExtension(String fileName) {
    String EMTY_CHR = "";
    File f = new File(fileName);
    int lastDotLoc = fileName.lastIndexOf(EXTN_CHR);
    if (isDirOrHiddenOrHasNoDot(f)) {
      return EMTY_CHR;
    } else {
      if (lastDotLoc > fileName.indexOf(File.separatorChar)) {
        return fileName.substring(lastDotLoc);
      } else {
        return EMTY_CHR;
      }
    }
  }

  /**
   * if the dirPath ends in a '/' it does nothing otherwise it adds a '/'
   * 
   * @param dirPath
   * @return
   */
  public static String ensureSlash(String dirPath) {
    if (!dirPath.endsWith(File.separator)) {
      return dirPath + File.separatorChar;
    } else {
      return dirPath;
    }
  }

  /**
   * Return true if the file exists
   * 
   * @param path
   * @return true of false
   */
  public static boolean exists(String path) {
    File f = new File(path);
    if (f.exists()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * gets the canonical path for the file, or null if it cannot
   * 
   * @param file
   * @return the canonical path
   */
  public static String getCanonicalPath(File file) {
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  /**
   * get an input stream for the file that is fast, trading memory for speed
   * 
   * @param file
   * @return InputStream
   */
  public static InputStream fastStream(File file) {
    FileInputStream fis = null;
    FileChannel ch = null;
    byte[] byteArray = null;
    try {
      fis = new FileInputStream(file);
      if (fis != null) {
        ch = fis.getChannel();
        if (ch.size() > 1000000000) {
        	return new BufferedInputStream(fis, BUFFER_SIZE);
        }
        MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, 0L, ch.size());
        byteArray = new byte[mb.capacity()];
        int got;
        while (mb.hasRemaining()) {
          got = Math.min(mb.remaining(), byteArray.length);
          mb.get(byteArray, 0, got);
        }
      }
    } catch (FileNotFoundException fnfe) {
      throw new IOError(fnfe);
    } catch (IOException ioe) {
      throw new IOError(ioe);
    } finally {
      if (ch != null) {
        try {
          ch.close();
          fis.close();
        } catch (IOException ioe2) {
        }
      }
    }
    return new ByteArrayInputStream(byteArray);
  }

  /**
   * Auto-detects compression format and decompresses.
   * Returns original bytes if magic number does not correspond to a compression scheme.
   * Handles many formats: "gz", "bzip2", "xz", or "pack200"
   * @param compressed 
   * @return
   * @throws Exception
   */
  public static byte[] uncompress(byte[] compressed) throws Exception {
      ByteArrayOutputStream unzipped = new ByteArrayOutputStream();
      CompressorInputStream input = null;
      try {
          input = new CompressorStreamFactory().createCompressorInputStream(new ByteArrayInputStream(compressed));
      } catch (CompressorException ce) {
          return compressed;
      }
      IOUtils.copy(input, unzipped);
      return unzipped.toByteArray();
  }
  
  /**
   * read the stdout of the process into a string and return when the function returns the process
   * has completed the stderr of the process is ignored.
   * Consider readProcessAsStringPair instead if the process may write to stderr.
   * 
   * @param proc
   * @return String
   * @throws InterruptedException
   */
  public static String readProcessAsString(Process proc) throws InterruptedException {
    BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    StringBuilder sb = new StringBuilder();
    String line = null;
    try {
      while ((line = br.readLine()) != null) {
        sb.append(line);
        sb.append("\n");
      }
    } catch (IOException e) {
      throw new IOError(e);
    }
    return sb.toString();
  }

	public static class FileIterable implements Iterable<File> {
		File root = null;
		Random rand = null;
		public FileFilter filter;
		public FileIterable(File root, FileFilter filter) {
			this.root = root;
			this.filter = filter;
		}
		public FileIterable(File root, FileFilter filter, boolean shuffle) {
			this.root = root;
			this.filter = filter;
			this.rand = shuffle ? new Random() : null;
		}
		public FileIterable(File root, FileFilter filter, Random rand) {
			this.root = root;
			this.filter = filter;
			this.rand = rand;
		}
		public FileIterable(File root) {
			this.root = root;
		}
		@Override
		public Iterator<File> iterator() {
			return new FileIterator(root,filter,rand);
		}
	}
  
  public static class FileIterator implements Iterator<File> {

    private Queue<File> queue = new LinkedList<File>();

    private FileFilter filter;

    private Random rand;

    private File ptr = null;

    public FileIterator(File root, FileFilter filter, Random rand) {
      queue.add(root);
      this.filter = filter;
      this.rand = rand;
    }

    public FileIterator(File root, FileFilter filter) {
      queue.add(root);
      this.filter = filter;
    }

    public FileIterator(File root) {
      this(root, null);
    }

    public File peek() {
      if (ptr == null) {
        ptr = next();
      }
      return ptr;
    }

    @Override
    public boolean hasNext() {
      if (ptr != null) {
        return true;
      }
      ptr = next();
      return ptr != null ? true : false;
    }

    @Override
    public File next() {
      if (ptr != null) {
        File file = ptr;
        ptr = null; // reset queue pointer
        return file;
      }
      if (queue.isEmpty()) {
        return null;
      } else {
        File f = queue.remove();
        if (f.isFile()) {
          if ((filter == null) || (filter != null && filter.accept(f))) {
            return f;
          } else {
            return next();
          }
        } else {
          File[] filesInDir = f.listFiles();
          if (filesInDir == null) {
        	  throw new RuntimeException("Null files when listing: "+f.getAbsolutePath());
          }
          try {
	          Arrays.sort(filesInDir, new Comparator<File>() {
		            @Override
		            public int compare(File thisFile, File thatFile) {
		              if (thisFile.isDirectory()) {
		                if (!thatFile.isDirectory()) {
		                  return 1;
		                } else {
		                  return thisFile.compareTo(thatFile);
		                }
		              } else if (thatFile.isDirectory()) {
		                return -1;
		              } else {
		                return thisFile.compareTo(thatFile);
		              }
		            }
		          });
        	  if (rand != null) {
        		  ArrayList<File> files = new ArrayList<File>(Arrays.asList(filesInDir));
        		  Collections.shuffle(files, rand);
        		  filesInDir = files.toArray(filesInDir);
        	  }
          } catch (NullPointerException npe) {
        	  throw new RuntimeException("NPE sorting files from "+f.getAbsolutePath()+":\n"+Lang.stringList(filesInDir, "\n"), npe);
          }
          List<File> files = Arrays.asList(filesInDir);
          for (File file : files) {
            queue.add(file);
          }
          return next();
        }
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not Implemented");
    }
  }
  
	/**
	 * constructs PrintStream to write to the file called filename, creating any parent directories, and gzipping the output if the filename ends with ".gz"
	 * @param filename
	 * @return
	 */
	public static PrintStream getFilePrintStream(String filename) {
		try {
			File file = new File(filename);
			ensureWriteable(file);
			OutputStream os = new FileOutputStream(file);
			if (filename.endsWith(".gz")) {
				os = new GZIPOutputStream(os);
			}			
			PrintStream out = new PrintStream(os);
			return out;
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	private static class FileLineIterable implements Iterable<String> {
		final String filename;
		final boolean skipComments;
		final boolean skipBlank;
		FileLineIterable(String filename, boolean skipComments, boolean skipBlank) {
			this.filename = filename; 
			this.skipComments = skipComments;
			this.skipBlank = skipBlank;
		}
		public Iterator<String> iterator() {
			return new FileLineIterator(filename, skipComments, skipBlank);
		}
	}
	private static class FileLineIterator implements Iterator<String> {
		BufferedReader reader;
		String line = null;
		final boolean skipComments;
		final boolean skipBlank;
		FileLineIterator(String filename, boolean skipComments, boolean skipBlank) {
			this.skipComments = skipComments;
			this.skipBlank = skipBlank;
			try {
				InputStream is = getInputStream(filename);
				reader = new BufferedReader(new InputStreamReader(is));
			} catch (Exception e) {throw new Error(e);}
			nextLine();
		}
		private void nextLine() {
			try {
				if (reader == null) return;
				line = reader.readLine();
				if (line == null) {
					reader.close();
					reader = null;
				}
			} catch (Exception e) {throw new Error(e);}
			if (skipComments && line != null && line.startsWith("//")) {
				nextLine();
			} else if (skipBlank && line != null && line.length() == 0) {
				nextLine();
			}
		}
		
		public boolean hasNext() {
			return line != null;
		}

		public String next() {
			String curLine = line;
			nextLine();
			return curLine;
		}
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	public static Iterable<String> getRawLines(String filename) {
		return new FileLineIterable(filename, false, false);
	}
	
	public static Iterable<String> getLines(String filename) {
		return new FileLineIterable(filename, false, true);
	}	
	
	public static Iterable<String> getNonCommentLines(String filename) {
		return new FileLineIterable(filename, true, true);
	}
	
	public static InputStream getResourceAsStream(String resource) {
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			if (loader == null) {
				loader = ClassLoader.getSystemClassLoader();
			}
			return loader.getResourceAsStream(resource);
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	
	public static String readResourceAsString(String resource) {
		InputStream is = getResourceAsStream(resource);
		if (is == null) {
			throw new IllegalArgumentException("Not found: "+resource);
		}
		return readStreamAsString(is);
	}
	
	public static String getFileExists(String... files) {
		for (String file : files) {
			if (FileUtil.exists(file))
				return file;
		}
		return null;
	}
	
	public static DataOutputStream getDataOutput(String filename) {
		try {
			DataOutputStream out = new DataOutputStream(getOutputStream(new File(filename)));
			return out;
		} catch (Exception e) {
			throw new Error(e);
		}		
	}
	public static DataInputStream getDataInput(String filename) {
		try {
			File file = new File(filename);	
			DataInputStream in = new DataInputStream(getInputStream(file));
			return in;
		} catch (Exception e) {
			throw new Error(e);
		}			
	}
	public static void ensureWorldReadable(File f) {
		File parent = f.getParentFile();
		if (parent != null && !parent.exists()) {
			ensureWorldReadable(parent);
			parent.mkdir();
			parent.setReadable(true, false);
			parent.setExecutable(true,false);
		}
	}
	public static void closeOutputStream(OutputStream os) {
		if (os != null) {
			try { os.close();
			} catch(Exception e) {}
		}
	}
	
	public static byte[] stringToBytes(String str) {
		try {
			ByteArrayOutputStream o = new ByteArrayOutputStream();
			GZIPOutputStream gz = new GZIPOutputStream(o);
			gz.write(str.getBytes());
			gz.close();
		return o.toByteArray();
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	public static String bytesToString(byte[] bs) {
		try {
			ByteArrayInputStream i = new ByteArrayInputStream(bs);
			GZIPInputStream gz = new GZIPInputStream(i);
			return readStreamAsString(gz);
		} catch (Exception e) {
			throw new Error(e);
		}
	}
	/**
	 * 
	 * @param process
	 * @return <stdout, stderr>
	 */
	public static Pair<String,String> readProcessAsStringPair(Process process) {
		final StringBuilder bufout = new StringBuilder();
		final StringBuilder buferr = new StringBuilder();
		StreamEater e1 = StreamEater.eatStream(
				new BufferedReader(new InputStreamReader(process.getInputStream())), 
				o -> bufout.append(o).append('\n'));
		StreamEater e2 = StreamEater.eatStream(
				new BufferedReader(new InputStreamReader(process.getErrorStream())),
				o -> buferr.append(o).append('\n'));
		try {
			process.waitFor();
			e1.join();
			e2.join();
		} catch (Exception e) {
			throw new Error(e);
		}
		return Pair.of(bufout.toString(), buferr.toString());
	}
	

	/**
	 * 
	 * @param fileName
	 */
	public static void deleteFileOrFolder(String fileName) {
		File tmp = new File(fileName);
		try {
			if (tmp.exists()) {
				// if deletion of a folder is unsuccessful, delete each individual
				// files inside the folder
				if (!tmp.delete()) {
					for (File f : tmp.listFiles()) {
						if (f.isFile())
							f.delete();
						else
							deleteFileOrFolder(f.getCanonicalPath());
					}
	
					tmp.delete();
				}
			}
		} catch (IOException ioe) {
			throw new Error(ioe);
		}
	}
}
