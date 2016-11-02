#!/usr/bin/python

#Description:
#---------------------
#Fetch Data from Mysql Database
#---------------------
#Create by coder-james

import os.path as op
from lib.db import mysql
import lib.conf as conf
from time import time

savefile="data_10mon.txt"
indicators="id;app_id;app_name;app_start_time;app_finish_time;user_sk;task_sum;job_id;job_type;stage_id;duration_sum;duration_avg;duration_variance;duration_min;duration_25th_percentile;duration_median;duration_75th_percentile;duration_95th_percentile;duration_99th_percentile;duration_max;jvm_gc_time_sum;jvm_gc_time_avg;jvm_gc_time_variance;jvm_gc_time_min;jvm_gc_time_25th_percentile;jvm_gc_time_median;jvm_gc_time_75th_percentile;jvm_gc_time_95th_percentile;jvm_gc_time_99th_percentile;jvm_gc_time_max;result_serialization_time_sum;result_serialization_time_avg;result_serialization_time_variance;result_serialization_time_min;result_serialization_time_25th_percentile;result_serialization_time_median;result_serialization_time_75th_percentile;result_serialization_time_95th_percentile;result_serialization_time_99th_percentile;result_serialization_time_max;getting_result_time_sum;getting_result_time_avg;getting_result_time_variance;getting_result_time_min;getting_result_time_25th_percentile;getting_result_time_median;getting_result_time_75th_percentile;getting_result_time_95th_percentile;getting_result_time_99th_percentile;getting_result_time_max;executor_deserialize_time_sum;executor_deserialize_time_avg;executor_deserialize_time_variance;executor_deserialize_time_min;executor_deserialize_time_25th_percentile;executor_deserialize_time_median;executor_deserialize_time_75th_percentile;executor_deserialize_time_95th_percentile;executor_deserialize_time_99th_percentile;executor_deserialize_time_max;scheduler_delay_sum;scheduler_delay_avg;scheduler_delay_variance;scheduler_delay_min;scheduler_delay_25th_percentile;scheduler_delay_median;scheduler_delay_75th_percentile;scheduler_delay_95th_percentile;scheduler_delay_99th_percentile;scheduler_delay_max;remote_blocks_fetched_sum;local_blocks_fetched_sum;fetch_wait_time_sum;remote_bytes_read_sum;local_bytes_read_sum;total_records_read_sum;shuffle_bytes_written_sum;shuffle_write_time_sum;shuffle_records_written_sum;memory_bytes_spilled_sum;disk_bytes_spilled_sum;create_time;stage_submission_time;stage_completion_time;job_completion_time;job_submission_time;spark_scheduler_mode;task_failure_num;task_success_num;spark_executor_memory;task_total_time;result_size;app_type;spark_streaming_internal_batch_time;stage_name;stage_attempt_id;input_records_read_sum;input_bytes_read_sum;output_records_written_sum;output_bytes_written_sum;is_aggregated;app_attempt_id"
#2016.11.1  Unix timestamp
end_date="14779584000" + "000"
#2016.10.1
start_date="1475280000" + "000"
#need to compute a new value
rawlist=["stage_submission_time","stage_completion_time","job_submission_time","job_completion_time"]

def fetch():
  db = mysql()
  sql = "select * from spark_task_metrics_summary  \
           where app_start_time > " + start_date + " and app_start_time < " + end_date + \
           " order by app_start_time;"
  results = db.fetchall(sql)
  cols = indicators.split(";")
  cols_dic = {name:i for i,name in enumerate(cols)}
  t1 = time()
  with open(op.join(conf.DATA_DIR, savefile), "w") as sfile:
    top_str = ""
    for colname in cols:
      if colname not in rawlist:
        top_str += colname + ","
    top_str +="stage_time,job_time\n"
    sfile.write(top_str)
    skip_index = [cols_dic[name] for name in rawlist]
    count={}
    for row in results:
      values = []
      for i,item in enumerate(row):
        if i not in skip_index:
          values.append(str(item))
      for i in range(0, len(rawlist), 2):
        va = long(row[cols_dic[rawlist[i + 1]]]) - long(row[cols_dic[rawlist[i]]])
        if va < 0:
          values.append("0")
          if i in count: count[i]+=1
          else: count[i]=1
        else:
	  values.append(str(va))
      sfile.write(",".join(values) + "\n")
      #for i,item in enumerate(row):
      #  print cols[i],item
  print "%s s---%s records have been saved to %s" %(time()-t1, len(results), savefile)
  print count
  db.close()
  
if __name__ == "__main__":
  assert start_date < end_date
  fetch()
  """
  1.2378680706 s---15455 records have been saved to data_10mon.txt
  """
