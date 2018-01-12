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

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.ibm.research.ai.ki.util.*;

/**
 * Writes items to a set of files in a root directory. 
 * The items are split over files according to itemsPerFile. 
 * The files are nested into directories to avoid exceeding FILES_PER_DIR.
 * 
 * @author mrglass
 *
 * @param <S>
 * @param <O>
 */
public abstract class MultiFileWriter<S extends AutoCloseable, O> implements AutoCloseable {

	/**
	 * The extension for the files to be written
	 * @return
	 */
	protected abstract String getExt();

	/**
	 * write the object to the stream
	 * @param stream
	 * @param obj
	 */
	protected abstract void write(S stream, O obj) throws IOException;

	/**
	 * get the stream we will write to
	 * @param f
	 * @return
	 */
	protected abstract S getStream(File f) throws IOException;

	
	public static final int FILES_PER_DIR = 100;
	private static final int lpadLen = (int) Math.ceil(Math.log10(FILES_PER_DIR));

	public final String partialExt;
	public final String finishedExt;

	protected final int itemsPerFile;
	protected String currentRelativeFile = null;
	protected int itemsInFile = 0;
	protected File rootDir;
	protected ArrayList<MutableInteger> currentDirectoryCounts = new ArrayList<>();
	protected S oos = null;
	protected final boolean overwrite;

	// also automatically creates subdirectories (and move already written files to a first subdir)
	public MultiFileWriter(File rootDir, int itemsPerFile, boolean overwrite) {
		this.rootDir = rootDir;
		this.itemsPerFile = itemsPerFile;
		currentDirectoryCounts.add(new MutableInteger(0));
		this.overwrite = overwrite;
		if (rootDir.exists() && !rootDir.isDirectory())
			throw new IllegalArgumentException("Not a directory! " + rootDir);
		if (rootDir.exists() && rootDir.listFiles().length > 0 && !overwrite)
			throw new IllegalArgumentException("Directory not empty! " + rootDir);
		
		String ext = getExt();
		if (ext.length() > 0 && !ext.startsWith("."))
			ext = "."+ext;
		this.finishedExt = ext;
		this.partialExt = ".partial"+ext;
	}

	public MultiFileWriter(File rootDir) {
		this(rootDir, 1000, false);
	}

	public synchronized void write(O doc) {
		try {
			if (rootDir == null)
				throw new Error("already closed");
			if (oos == null)
				oos = makeStream();
			if (itemsInFile >= itemsPerFile)
				nextStream();
			this.write(oos, doc);
			++itemsInFile;
		} catch (Exception e) {
			Lang.error(e);
		}
	}

	@Override
	public synchronized void close() {
		try {
			if (oos != null) {
				oos.close();
				Files.move(Paths.get(rootDir.getAbsolutePath(), currentRelativeFile + partialExt),
						Paths.get(rootDir.getAbsolutePath(), currentRelativeFile + finishedExt));
			}
			oos = null;
			rootDir = null;
		} catch (Exception e) {
			// ignore
		}
	}

	protected void nextStream() {
		try {
			oos.close();
			Files.move(Paths.get(rootDir.getAbsolutePath(), currentRelativeFile + partialExt),
					Paths.get(rootDir.getAbsolutePath(), currentRelativeFile + finishedExt));

			for (MutableInteger c : currentDirectoryCounts) {
				c.value += 1;
				if (c.value >= FILES_PER_DIR)
					c.value = 0; // we will create new dir
				else
					break;
			}
			if (currentDirectoryCounts.get(currentDirectoryCounts.size() - 1).value == 0)
				deepenDirectories();
			itemsInFile = 0;
			oos = makeStream();

		} catch (Exception e) {
			Lang.error(e);
		}
	}

	static String dirName(int dirNum) {
		return Lang.LPAD(String.valueOf(dirNum), '0', lpadLen);
	}

	protected S makeStream() throws Exception {
		StringBuilder buf = new StringBuilder();
		for (int i = currentDirectoryCounts.size() - 1; i >= 0; --i)
			buf.append(dirName(currentDirectoryCounts.get(i).value)).append(File.separator);
		buf.setLength(buf.length() - 1);
		currentRelativeFile = buf.toString();
		String baseFilename = FileUtil.ensureSlash(rootDir.getAbsolutePath()) + currentRelativeFile;

		if (!overwrite
				&& (FileUtil.exists(baseFilename + finishedExt) || FileUtil.exists(baseFilename + partialExt)))
			throw new Error("File already exists: " + baseFilename);

		File file = new File(baseFilename + partialExt);
		FileUtil.ensureWriteable(file);
		return this.getStream(file);
	}

	protected void deepenDirectories() {
		try {
			Path tempRoot = Paths.get(rootDir.getParent(), rootDir.getName() + "___temp");
			if (FileUtil.exists(tempRoot.toString()))
				throw new Error("temp dir for deepening exists!");
			Files.move(Paths.get(rootDir.getAbsolutePath()), tempRoot);
			rootDir.mkdirs();
			Files.move(tempRoot, Paths.get(rootDir.getAbsolutePath(), dirName(0)));
			currentDirectoryCounts.add(new MutableInteger(1));
		} catch (Exception e) {
			Lang.error(e);
		}
	}

}
