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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ReservationId;

/**
 * Due to the inconsistencies in Hadoop API, it is sometimes required to pass in a full-blown Job
 * while only a more generic interface (e.g. a JobContext) is provided. Further, in such cases
 * the Job is only used to get the Configuration out of it. This dummy class serves as a holder
 * for that Configuration that also matches the expected type.
 */
public class HadoopJobHolder extends Job {
    private final Configuration configuration;

    public HadoopJobHolder(Configuration configuration) throws IOException {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    // All the methods below are not expected to be ever invoked, and so throw an UnsupportedOperationException

    @Override
    public JobStatus getStatus() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public JobStatus.State getJobState() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getTrackingURL() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getJobFile() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public long getStartTime() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public long getFinishTime() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getSchedulingInfo() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public JobPriority getPriority() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getJobName() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getHistoryUrl() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean isRetired() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Cluster getCluster() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public TaskReport[] getTaskReports(TaskType type) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public float mapProgress() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public float reduceProgress() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public float cleanupProgress() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public float setupProgress() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean isComplete() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean isSuccessful() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void killJob() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setPriority(JobPriority priority) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom, int numEvents) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public org.apache.hadoop.mapred.TaskCompletionEvent[] getTaskCompletionEvents(int startFrom) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean killTask(TaskAttemptID taskId, boolean shouldFail) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void killTask(TaskAttemptID taskId) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void failTask(TaskAttemptID taskId) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Counters getCounters() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String[] getTaskDiagnostics(TaskAttemptID taskid) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setNumReduceTasks(int tasks) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setWorkingDirectory(Path dir) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setInputFormatClass(Class<? extends InputFormat> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setOutputFormatClass(Class<? extends OutputFormat> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMapperClass(Class<? extends Mapper> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setJarByClass(Class<?> cls) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setJar(String jar) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setUser(String user) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setCombinerClass(Class<? extends Reducer> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setReducerClass(Class<? extends Reducer> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setPartitionerClass(Class<? extends Partitioner> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMapOutputKeyClass(Class<?> theClass) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMapOutputValueClass(Class<?> theClass) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setOutputKeyClass(Class<?> theClass) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setOutputValueClass(Class<?> theClass) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setCombinerKeyGroupingComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setSortComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setGroupingComparatorClass(Class<? extends RawComparator> cls) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setJobName(String name) throws IllegalStateException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setSpeculativeExecution(boolean speculativeExecution) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMapSpeculativeExecution(boolean speculativeExecution) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setReduceSpeculativeExecution(boolean speculativeExecution) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setJobSetupCleanupNeeded(boolean needed) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setCacheArchives(URI[] archives) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setCacheFiles(URI[] files) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void addCacheArchive(URI uri) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void addCacheFile(URI uri) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void addFileToClassPath(Path file) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void addArchiveToClassPath(Path archive) throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void createSymlink() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMaxMapAttempts(int n) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setMaxReduceAttempts(int n) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setProfileEnabled(boolean newValue) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setProfileParams(String value) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setProfileTaskRange(boolean isMap, String newValue) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setCancelDelegationTokenUponJobCompletion(boolean value) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void submit() throws IOException, InterruptedException, ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean monitorAndPrintJob() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean isUber() throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public ReservationId getReservationId() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setReservationId(ReservationId reservationId) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public JobID getJobID() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public void setJobID(JobID jobId) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public int getNumReduceTasks() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<?> getOutputKeyClass() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<?> getOutputValueClass() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public RawComparator<?> getSortComparator() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getJar() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean getTaskCleanupNeeded() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean getSymlink() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Path[] getArchiveClassPaths() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Path[] getFileClassPaths() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String[] getArchiveTimestamps() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String[] getFileTimestamps() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public int getMaxMapAttempts() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public int getMaxReduceAttempts() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public boolean getProfileEnabled() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getProfileParams() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public String getUser() {
        throw new UnsupportedOperationException("Should never be invoked");
    }

    @Override
    public Credentials getCredentials() {
        throw new UnsupportedOperationException("Should never be invoked");
    }
}
