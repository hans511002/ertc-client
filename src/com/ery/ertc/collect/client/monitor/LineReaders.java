package com.ery.ertc.collect.client.monitor;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.InputStream;

import com.ery.base.support.log4j.LogUtils;

/**
 * A class that provides a line reader from an input stream. Depending on the
 * constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated line.
 */
public class LineReaders {
	public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
	private int bufferSize = DEFAULT_BUFFER_SIZE;
	private InputStream in;
	private byte[] buffer;
	// the number of bytes of real data in the buffer
	private int bufferLength = 0;
	// the current position in the buffer
	private int bufferPosn = 0;

	private static final byte CR = '\r';
	private static final byte LF = '\n';

	// The line delimiter
	private final byte[] recordDelimiterBytes;

	protected int perFileSkipRowNum = 0;
	int curFileSkipNum = 0;

	public LineReaders(InputStream in, int skipNum) {
		this(in, DEFAULT_BUFFER_SIZE, skipNum);
	}

	public LineReaders(InputStream in, int bufferSize, int skipNum) {
		this(in, bufferSize, null, skipNum);
	}

	public LineReaders(InputStream in, byte[] recordDelimiterBytes, int skipNum) {
		this(in, DEFAULT_BUFFER_SIZE, recordDelimiterBytes, skipNum);
	}

	public LineReaders(InputStream in, int bufferSize, byte[] recordDelimiterBytes, int skipNum) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.perFileSkipRowNum = skipNum;
		this.buffer = new byte[this.bufferSize];
		this.recordDelimiterBytes = recordDelimiterBytes;
		try {
			skipFileNum(new StringBuffer(), 1 << 20, 1 << 20);
		} catch (IOException e) {
			LogUtils.error("跳过记录异常：", e);
		}
	}

	public void close() throws IOException {
		in.close();
	}

	public InputStream getInputStream() {
		return this.in;
	}

	public int readLine(StringBuffer str, int maxLineLength, int maxBytesToConsume) throws IOException {
		int readsize = -1;
		if (this.recordDelimiterBytes != null) {
			readsize = readCustomLine(str, maxLineLength, maxBytesToConsume);
		} else {
			readsize = readDefaultLine(str, maxLineLength, maxBytesToConsume);
		}
		return readsize;
	}

	private boolean skipFileNum(StringBuffer str, int maxLineLength, int maxBytesToConsume) throws IOException {
		int rowNum = 0;
		int readsize = 1;
		while (rowNum++ < this.perFileSkipRowNum && readsize > 0) {
			if (this.recordDelimiterBytes != null) {
				readsize = readCustomLine(str, maxLineLength, maxBytesToConsume);
			} else {
				readsize = readDefaultLine(str, maxLineLength, maxBytesToConsume);
			}
		}
		return true;
	}

	/**
	 * Read a line terminated by one of CR, LF, or CRLF.
	 */
	private int readDefaultLine(StringBuffer str, int maxLineLength, int maxBytesToConsume) throws IOException {
		str.setLength(0);
		int txtLength = 0; // tracks str.getLength(), as an optimization
		int newlineLength = 0; // length of terminating newline
		boolean prevCharCR = false; // true of prev char was CR
		long bytesConsumed = 0;
		do {
			int startPosn = bufferPosn; // starting from where we left off the
										// last time
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				if (prevCharCR)
					++bytesConsumed; // account for CR from previous read
				bufferLength = in.read(buffer);
				if (bufferLength <= 0)
					break; // EOF
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
																// newline
				if (buffer[bufferPosn] == LF) {
					newlineLength = (prevCharCR) ? 2 : 1;
					++bufferPosn; // at next invocation proceed from following
									// byte
					break;
				}
				if (prevCharCR) { // CR + notLF, we are at notLF
					newlineLength = 1;
					break;
				}
				prevCharCR = (buffer[bufferPosn] == CR);
			}
			int readLength = bufferPosn - startPosn;
			if (prevCharCR && newlineLength == 0)
				--readLength; // CR at the end of the buffer
			bytesConsumed += readLength;
			int appendLength = readLength - newlineLength;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(new String(buffer, startPosn, appendLength));
				txtLength += appendLength;
			}
		} while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

		if (bytesConsumed > (long) Integer.MAX_VALUE)
			throw new IOException("Too many bytes before newline: " + bytesConsumed);
		return (int) bytesConsumed;
	}

	/**
	 * Read a line terminated by a custom delimiter.
	 */
	private int readCustomLine(StringBuffer str, int maxLineLength, int maxBytesToConsume) throws IOException {
		str.setLength(0);
		int txtLength = 0; // tracks str.getLength(), as an optimization
		long bytesConsumed = 0;
		int delPosn = 0;
		do {
			int startPosn = bufferPosn; // starting from where we left off the
										// last
			// time
			if (bufferPosn >= bufferLength) {
				startPosn = bufferPosn = 0;
				bufferLength = in.read(buffer);
				if (bufferLength <= 0)
					break; // EOF
			}
			for (; bufferPosn < bufferLength; ++bufferPosn) {
				if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
					delPosn++;
					if (delPosn >= recordDelimiterBytes.length) {
						bufferPosn++;
						break;
					}
				} else {
					delPosn = 0;
				}
			}
			int readLength = bufferPosn - startPosn;
			bytesConsumed += readLength;
			int appendLength = readLength - delPosn;
			if (appendLength > maxLineLength - txtLength) {
				appendLength = maxLineLength - txtLength;
			}
			if (appendLength > 0) {
				str.append(new String(buffer, startPosn, appendLength));
				txtLength += appendLength;
			}
		} while (delPosn < recordDelimiterBytes.length && bytesConsumed < maxBytesToConsume);
		if (bytesConsumed > (long) Integer.MAX_VALUE)
			throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
		return (int) bytesConsumed;
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @param maxLineLength
	 *            the maximum number of bytes to store into str.
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(StringBuffer str, int maxLineLength) throws IOException {
		return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	/**
	 * Read from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given line
	 * @return the number of bytes read including the newline
	 * @throws IOException
	 *             if the underlying stream throws
	 */
	public int readLine(StringBuffer str) throws IOException {
		return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}

}
