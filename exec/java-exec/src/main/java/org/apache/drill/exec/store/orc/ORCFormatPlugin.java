/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.orc;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.parquet.Metadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class ORCFormatPlugin implements FormatPlugin {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ORCFormatPlugin.class);
  private static final String DEFAULT_NAME = "orc";

  private static final List<Pattern> PATTERNS = Lists.newArrayList(
      Pattern.compile(".*\\.parquet$"));

  private final DrillbitContext context;
  private final Configuration fsConf;
  private final ORCFormatMatcher formatMatcher;
  private final ORCFormatConfig config;
  private final StoragePluginConfig storageConfig;
  private final String name;

  public ORCFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                             StoragePluginConfig storageConfig){
    this(name, context, fsConf, storageConfig, new ORCFormatConfig());
  }
  public ORCFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
                             StoragePluginConfig storageConfig, ORCFormatConfig formatConfig){
    this.context = context;
    this.config = formatConfig;
    this.formatMatcher = new ORCFormatPlugin.ORCFormatMatcher(this, config);
    this.storageConfig = storageConfig;
    this.fsConf = fsConf;
    this.name = name == null ? DEFAULT_NAME : name;
  }


  public boolean supportsRead() {
    return true;
  }

  public boolean supportsWrite() {
    return false;
  }


  public boolean supportsAutoPartitioning() {
    return false;
  }

  public FormatMatcher getMatcher() {
    return this.formatMatcher;
  }

  public AbstractWriter getWriter(PhysicalOperator child, String location, List<String> partitionColumns) throws IOException {
    return null;
  }

  public Set<StoragePluginOptimizerRule> getOptimizerRules() {
    return Sets.newHashSet();
  }

  public AbstractGroupScan getGroupScan(String userName, FileSelection selection, List<SchemaPath> columns)
      throws IOException {
    return new ORCGroupScan(userName, selection, this, selection.selectionRoot, selection.cacheFileRoot, columns);
  }

  public FormatPluginConfig getConfig() {
    return this.config;
  }

  public StoragePluginConfig getStorageConfig() {
    return this.storageConfig;
  }

  public Configuration getFsConf() {
    return this.fsConf;
  }
  public DrillbitContext getContext() {
    return context;
  }
  public String getName() {
    return name;
  }


  private static class ORCFormatMatcher extends BasicFormatMatcher {

    private final ORCFormatConfig formatConfig;
    private static final List<MagicString> MAGIC_STRINGS =
        Lists.newArrayList(new MagicString(0, "ORC".getBytes(Charset.forName("ASCII"))));
    public ORCFormatMatcher(ORCFormatPlugin plugin, ORCFormatConfig formatConfig) {
      super(plugin, PATTERNS, MAGIC_STRINGS);
      this.formatConfig = formatConfig;
    }

    @Override
    public boolean supportDirectoryReads() {
      return true;
    }

    @Override
    public DrillTable isReadable(DrillFileSystem fs, FileSelection selection,
                                 FileSystemPlugin fsPlugin, String storageEngineName, String userName)
        throws IOException {
      return super.isReadable(fs, selection, fsPlugin, storageEngineName, userName);
    }

    private Path getMetadataPath(FileStatus dir) {
      return new Path(dir.getPath(), Metadata.METADATA_FILENAME);
    }

    private boolean metaDataFileExists(FileSystem fs, FileStatus dir) throws IOException {
      return fs.exists(getMetadataPath(dir));
    }

    boolean isDirReadable(DrillFileSystem fs, FileStatus dir) {
      Path p = new Path(dir.getPath(), ParquetFileWriter.PARQUET_METADATA_FILE);
      try {
        if (fs.exists(p)) {
          return true;
        } else {

          if (metaDataFileExists(fs, dir)) {
            return true;
          }
          PathFilter filter = new DrillPathFilter();

          FileStatus[] files = fs.listStatus(dir.getPath(), filter);
          if (files.length == 0) {
            return false;
          }
          return super.isFileReadable(fs, files[0]);
        }
      } catch (IOException e) {
        logger.info("Failure while attempting to check for Parquet metadata file.", e);
        return false;
      }
    }
  }

}
