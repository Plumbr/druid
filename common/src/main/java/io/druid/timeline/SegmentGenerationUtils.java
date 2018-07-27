package io.druid.timeline;

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
