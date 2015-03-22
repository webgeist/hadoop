package org.apache.hadoop.streaming;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;

/**
 * Partitions the key space.
 * Created by pavel on 21.03.15.
 */
public class PipePartitioner extends PipeMapRed implements Partitioner {

  private byte[] partitionOutFieldSeparator;
  private byte[] partitionInputFieldSeparator;
  private int numOfPartitionOutputKeyFields = 1;

  public int getPartition(Object key, Object value, int numPartitions) throws IOException {

    LOG.info("getPartition key " + key);
    LOG.info("getPartition value " + value);

    ProcessBuilder pb = new ProcessBuilder(argSplit_[0],""+key,""+value, ""+numPartitions);
    Process p = pb.start();
    BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
    int ret = Integer.parseInt(in.readLine());
    LOG.info("value is : "+ret);
    return ret;
  }

  public void configure(JobConf job){
    super.configure(job);

    try {
      partitionOutFieldSeparator = job_.get("stream.partition.output.field.separator", "\t").getBytes("UTF-8");
      partitionInputFieldSeparator = job_.get("stream.partition.input.field.separator", "\t").getBytes("UTF-8");
      this.numOfPartitionOutputKeyFields = job_.getInt("stream.num.partition.output.key.fields", 1);
    }catch (UnsupportedEncodingException e) {
      throw new RuntimeException("The current system does not support UTF-8 encoding!", e);
    }
  }

  String getPipeCommand(JobConf job){
    String str = job.get("stream.partition.streamprocessor");
    if (str == null) {
      return str;
    }
    try {
      return URLDecoder.decode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.err.println("stream.partition.streamprocessor in jobconf not found");
      return null;
    }
  }

  boolean getDoPipe() {
    String argv = getPipeCommand(job_);
    // Currently: null is identity reduce. REDUCE_NONE is no-map-outputs.
    return (argv != null) && !StreamJob.REDUCE_NONE.equals(argv);
  }

  @Override
  public byte[] getInputSeparator() {
    return partitionInputFieldSeparator;
  }

  @Override
  public byte[] getFieldSeparator() {
    return partitionOutFieldSeparator;
  }

  @Override
  public int getNumOfKeyFields() {
    return numOfPartitionOutputKeyFields;
  }

  @Override
  InputWriter createInputWriter() throws IOException {
    return super.createInputWriter(reduceInputWriterClass_);
  }

  @Override
  OutputReader createOutputReader() throws IOException {
    return super.createOutputReader(reduceOutputReaderClass_);
  }
}
