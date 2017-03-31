/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;

import java.io.File;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

  /**
   * System property key to get base test directory value
   */
  public static final String BASE_TEST_DIRECTORY_KEY =
      "test.build.data.basedirectory";

  /**
   * Default base directory for test output.
   */
  public static final String DEFAULT_BASE_TEST_DIRECTORY = "target/test-data";

  /**
   * Directory where we put the data for this instance of TestUtil.
   */
  private File dataTestDir = null;

  /**
   * @return Where to write test data on local filesystem, specific to
   * the test.  Useful for tests that do not use a cluster.
   * Creates it if it does not exist already.
   */
  public File getDataTestDir() {
    if (this.dataTestDir == null) {
      setupDataTestDir();
    }
    return new File(this.dataTestDir.getAbsolutePath());
  }

  /**
   * @param subdirName
   * @return Path to a subdirectory named <code>subdirName</code> under
   * {@link #getDataTestDir()}.
   * Creates it if it does not exist already.
   */
  public File getDataTestDir(final String subdirName) {
    File ret = new File(getDataTestDir(), subdirName);
    ret.mkdirs();
    return ret;
  }

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #DEFAULT_BASE_TEST_DIRECTORY}
   * Should not be used by the unit tests, hence its's private.
   * Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private File getBaseTestDir() {
    String PathName = System.getProperty(
        BASE_TEST_DIRECTORY_KEY, DEFAULT_BASE_TEST_DIRECTORY);

    return new File(PathName);
  }

  /**
   * Sets up a directory for a test to use.
   *
   * @return New directory path, if created.
   */
  protected File setupDataTestDir() {
    if (this.dataTestDir != null) {
      LOG.warn("Data test dir already setup in " +
          dataTestDir.getAbsolutePath());
      return null;
    }
    File testPath = getRandomDir();
    this.dataTestDir = new File(testPath.toString()).getAbsoluteFile();
    if (deleteOnExit()) this.dataTestDir.deleteOnExit();
    dataTestDir.mkdirs();

    return testPath;
  }

  /**
   * @return A dir with a random (uuid) name under the test dir
   * @see #getBaseTestDir()
   */
  public File getRandomDir() {
    return new File(getBaseTestDir(), UUID.randomUUID().toString());
  }

  /**
   * @return True if we should delete testing dirs on exit.
   */
  boolean deleteOnExit() {
    String v = System.getProperty("ratis.testing.preserve.testdir");
    // Let default be true, to delete on exit.
    return v == null ? true : !Boolean.parseBoolean(v);
  }

}
