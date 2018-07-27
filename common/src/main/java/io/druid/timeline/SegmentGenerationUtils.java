package io.druid.timeline;

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
