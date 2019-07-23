/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.timeline;

/**
 * A segment with version containing _gen_ is considered to be a new generation of an existing segment, and the version
 * it is generated from (source version) matches the part before _gen_.
 * <p>
 * A generated segment has the following properties:
 * <ul>
 *   <li>
 *     It overshadows its base version segments per partition - if there is no generated segment with a specific
 *     partition number, then the source version segment with that partition is active.
 *   </li>
 *   <li>
 *     Its version is not used for allocating new segments internally - if it is the latest version for an interval,
 *     then the segment is allocated to the source version instead. Due to the previous property, the new allocated
 *     segment would still be active until a generated version of the same partition is created.
 *   </li>
 * </ul>
 */
public class SegmentGenerationUtils
{
  /**
   * In case the given version string represents a custom generation of some base version, returns that base version.
   * Otherwise returns <code>null</code>.
   */
  public static String getSourceVersion(String version)
  {
    int generationIndex = version.indexOf("_gen_");

    if (generationIndex >= 0) {
      return version.substring(0, generationIndex);
    } else {
      return null;
    }
  }
}
