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
package org.apache.hadoop.hive.ql.exec.spark;

import com.clearspring.analytics.util.Preconditions;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A cache with fixed buffer size for {@link BytesWritable}s. If the buffer is full,
 * new entries will be spill to disk. NOTE: this class is NOT thread safe.
 *
 * Use this class in the following pattern:
 *
 * <code>
 * HiveBytesWritableCache cache = new ...
 *
 * // Write entries to cache. May persist to disk.
 * while (...) {
 *   cache.add(..);
 * }
 *
 * // Done with writing. Start reading from cache.
 * cache.startRead();
 * for (BytesWritable bw : cache) {
 *   ...
 * }
 *
 * // Done with reading. Close and clear the cache.
 * cache.close();
 * </code>
 */
public class HiveBytesWritableCache implements Iterable<BytesWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBytesWritableCache.class.getName());

  private final List<BytesWritable> buffer;

  // Indicate whether we have flushed any data to disk.
  // If this is not set, then all data can be hold in a single buffer, and thus
  // no need to flush to disk and initialize input & output.
  private boolean flushed;

  private File parentFile;
  private File tmpFile;

  // The maximum buffer size, in bytes.
  // If this value is exceeded, the extra data will be spill to disk.
  private final long maxBufferSize;

  // The current buffer size. If this exceeds `maxBufferSize`, spill to disk.
  private long bufferSize;

  private Output output;

  HiveBytesWritableCache(long maxBufferSize) {
    this.flushed = false;
    this.buffer = new ArrayList<>();
    this.maxBufferSize = maxBufferSize;
    this.bufferSize = 0;
  }

  public void add(BytesWritable value) {
    if (bufferSize > maxBufferSize) {
      flushBuffer();
    }
    buffer.add(value);
    bufferSize += value.getCapacity();
  }

  /**
   * Start reading from the cache. MUST be called before calling any
   * of the iterator methods.
   */
  public void startRead() {
    if (flushed && !buffer.isEmpty()) {
      flushBuffer();
    }
    if (output != null) {
      output.close();
      output = null;
    }
  }

  @Override
  public Iterator<BytesWritable> iterator() {
    return new BytesWritableIterator(buffer);
  }

  /**
   * Close this cache. Idempotent.
   */
  public void close() {
    flushed = false;
    buffer.clear();
    bufferSize = 0;

    if (parentFile != null) {
      if (output != null) {
        try {
          output.close();
        } catch (Throwable e) {
          LOG.warn("Error when closing cache output.", e);
        }
        output = null;
      }
      FileUtil.fullyDelete(parentFile);
      parentFile = null;
      tmpFile = null;
    }
  }

  private void flushBuffer() {
    // Initialize output temporary file if not already set
    if (output == null) {
      try {
        setupOutput();
      } catch (IOException e) {
        close();
        throw new RuntimeException("Error when setting up output stream.", e);
      }
    }

    try {
      int i = 0;
      for (; i < buffer.size(); i++) {
        BytesWritable writable = buffer.get(i);
        writeValue(output, writable);
      }
      buffer.clear();
      flushed = true;
      bufferSize = 0;
    } catch (Exception e) {
      close();
      throw new RuntimeException("Error when spilling to disk", e);
    }
  }

  private void setupOutput() throws IOException {
    Preconditions.checkState(parentFile == null && tmpFile == null);
    while (true) {
      parentFile = File.createTempFile("hive-resultcache", "");
      if (parentFile.delete() && parentFile.mkdir()) {
        parentFile.deleteOnExit();
        break;
      }
      LOG.debug("Retry creating tmp result-cache directory...");
    }

    tmpFile = File.createTempFile("ResultCache", ".tmp", parentFile);
    LOG.info("ResultCache created temp file " + tmpFile.getAbsolutePath());
    tmpFile.deleteOnExit();

    FileOutputStream fos;
    fos = new FileOutputStream(tmpFile);
    output = new Output(fos);
  }

  private BytesWritable readValue(Input input) {
    return new BytesWritable(input.readBytes(input.readInt()));
  }

  private void writeValue(Output output, BytesWritable bytesWritable) {
    int size = bytesWritable.getLength();
    output.writeInt(size);
    output.writeBytes(bytesWritable.getBytes(), 0, size);
  }

  private class BytesWritableIterator implements Iterator<BytesWritable> {
    private Iterator<BytesWritable> it;
    private Input input = null;

    BytesWritableIterator(List<BytesWritable> buffer) {
      this.it = buffer.iterator();
      if (tmpFile != null) {
        try {
          FileInputStream fis = new FileInputStream(tmpFile);
          input = new Input(fis);
        } catch (IOException e) {
          close();
          throw new RuntimeException("Error when setting up input stream for tmp file: " + tmpFile, e);
        }
      }
    }

    @Override
    public boolean hasNext() {
      return it.hasNext() || hasMoreInput();
    }

    @Override
    public BytesWritable next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more next");
      }
      if (!it.hasNext()) {
        loadBuffer();
      }
      return it.next();
    }

    /**
     * Whether there's more data to load from disk
     */
    private boolean hasMoreInput() {
      return input != null && !input.eof();
    }

    private void loadBuffer() {
      long bufferSize = 0;
      List<BytesWritable> buffer = new ArrayList<>();
      while (bufferSize < maxBufferSize) {
        if (input.eof()) {
          input.close();
          input = null;
          break;
        }
        BytesWritable value = readValue(input);
        buffer.add(value);
        bufferSize += value.getCapacity();
      }
      it = buffer.iterator();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove is not supported");
    }
  }

}
