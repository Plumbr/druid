/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatasourceInputFormat extends InputFormat<NullWritable, InputRow>
{
  private static final Logger logger = new Logger(DatasourceInputFormat.class);

  public static final String CONF_INPUT_SEGMENTS = "druid.segments";
  public static final String CONF_DRUID_SCHEMA = "druid.datasource.schema";
  public static final String CONF_MAX_SPLIT_SIZE = "druid.datasource.split.max.size";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();

    String segmentsStr = Preconditions.checkNotNull(
        conf.get(CONF_INPUT_SEGMENTS),
        "No segments found to read"
    );
    List<WindowedDataSegment> segments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        segmentsStr,
        new TypeReference<List<WindowedDataSegment>>()
        {
        }
    );
    if (segments == null || segments.size() == 0) {
      throw new ISE("No segments found to read");
    }

    logger.info("segments to read [%s]", segmentsStr);

    long maxSize = conf.getLong(CONF_MAX_SPLIT_SIZE, 0);
    if (maxSize < 0) {
      long totalSize = 0;
      for (WindowedDataSegment segment : segments) {
        totalSize += segment.getSegment().getSize();
      }
      int mapTask = ((JobConf) conf).getNumMapTasks();
      if (mapTask > 0) {
        maxSize = totalSize / mapTask;
      }
    }

    if (maxSize > 0) {
      //combining is to happen, let us sort the segments list by size so that they
      //are combined appropriately
      Collections.sort(
          segments,
          new Comparator<WindowedDataSegment>()
          {
            @Override
            public int compare(WindowedDataSegment s1, WindowedDataSegment s2)
            {
              return Long.compare(s1.getSegment().getSize(), s2.getSegment().getSize());
            }
          }
      );
    }

    List<InputSplit> splits = Lists.newArrayList();

    List<WindowedDataSegment> list = new ArrayList<>();
    long size = 0;

    InputFormat<LongWritable, Text> fio = supplier.get();
    for (WindowedDataSegment segment : segments) {
      if (size + segment.getSegment().getSize() > maxSize && size > 0) {
        splits.add(toDataSourceSplit(list, fio, context));
        list = Lists.newArrayList();
        size = 0;
      }

      list.add(segment);
      size += segment.getSegment().getSize();
    }

    if (list.size() > 0) {
      splits.add(toDataSourceSplit(list, fio, context));
    }

    logger.info("Number of splits [%d]", splits.size());
    return splits;
  }

  @Override
  public RecordReader<NullWritable, InputRow> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException, InterruptedException
  {
    return new DatasourceRecordReader();
  }

  private Supplier<InputFormat<LongWritable, Text>> supplier = () -> new TextInputFormat()
  {
    //Always consider non-splittable as we only want to get location of blocks for the segment
    //and not consider the splitting.
    //also without this, isSplitable(..) fails with NPE because compressionCodecs is not properly setup.
    @Override
    protected boolean isSplitable(JobContext jobContext, Path file)
    {
      return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException
    {
      // to avoid globbing which needs input path should be hadoop-compatible (':' is not acceptable in path, etc.)
      List<FileStatus> statusList = Lists.newArrayList();
      for (Path path : FileInputFormat.getInputPaths(job)) {
        // load spec in segment points specifically zip file itself
        statusList.add(path.getFileSystem(job.getConfiguration()).getFileStatus(path));
      }
      return statusList;
    }
  };

  @VisibleForTesting
  DatasourceInputFormat setSupplier(Supplier<InputFormat<LongWritable, Text>> supplier)
  {
    this.supplier = supplier;
    return this;
  }

  private DatasourceInputSplit toDataSourceSplit(
          List<WindowedDataSegment> segments, InputFormat<LongWritable, Text> fio, JobContext jobContext
  )
  {
    String[] locations = getFrequentLocations(getLocations(segments, fio, jobContext));

    return new DatasourceInputSplit(segments, locations);
  }

  @VisibleForTesting
  static Stream<String> getLocations(
      final List<WindowedDataSegment> segments,
      final InputFormat<LongWritable, Text> fio,
      final JobContext jobContext
  )
  {
    return segments.stream().sequential().flatMap(
        (final WindowedDataSegment segment) -> {
          try {

            // For reasons unknown, the setInputPaths method requires the concrete class of Job as the first argument.
            // However, the InputFormat.getSplits() method is only given the abstract JobContext. To avoid a potentially
            // unsafe cast, we use the implementation detail: setInputPaths only takes the job's configuration.
            // Therefore, a dummy Job instance backed by the real configuration is made.
            FileInputFormat.setInputPaths(
                    new HadoopJobHolder(jobContext.getConfiguration()),
                    new Path(JobHelper.getURIFromSegment(segment.getSegment()))
            );

            return fio.getSplits(jobContext).stream().flatMap(
                (final InputSplit split) -> {
                  try {
                    return Arrays.stream(split.getLocations());
                  }
                  catch (final IOException e) {
                    logger.error(e, "Exception getting locations");
                    return Stream.empty();
                  } catch (InterruptedException e) {
                    logger.error(e, "Interrupted while getting locations");
                    return Stream.empty();
                  }
                }
            );
          }
          catch (final IOException e) {
            logger.error(e, "Exception getting splits");
            return Stream.empty();
          } catch (InterruptedException e) {
            logger.error(e, "Interrupted while getting splits");
            return Stream.empty();
          }
        }
    );
  }

  @VisibleForTesting
  static String[] getFrequentLocations(final Stream<String> locations)
  {
    final Map<String, Long> locationCountMap = locations.collect(
        Collectors.groupingBy(location -> location, Collectors.counting())
    );

    final Comparator<Map.Entry<String, Long>> valueComparator =
        Map.Entry.comparingByValue(Comparator.reverseOrder());

    final Comparator<Map.Entry<String, Long>> keyComparator =
        Map.Entry.comparingByKey();

    return locationCountMap
        .entrySet().stream()
        .sorted(valueComparator.thenComparing(keyComparator))
        .limit(3)
        .map(Map.Entry::getKey)
        .toArray(String[]::new);
  }
}
