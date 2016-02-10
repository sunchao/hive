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
package org.apache.hadoop.hive.common.metrics.common;

/**
 * This class defines some metrics generated by Hive processes.
 */
public class MetricsConstant {

  public static String JVM_PAUSE_INFO = "jvm.pause.info-threshold";
  public static String JVM_PAUSE_WARN = "jvm.pause.warn-threshold";
  public static String JVM_EXTRA_SLEEP = "jvm.pause.extraSleepTime";

  public static String OPEN_CONNECTIONS = "open_connections";
  public static String OPEN_OPERATIONS = "open_operations";
  public static final String CUMULATIVE_CONNECTION_COUNT = "cumulative_connection_count";

  public static final String JDO_ACTIVE_TRANSACTIONS = "active_jdo_transactions";
  public static final String JDO_ROLLBACK_TRANSACTIONS = "rollbacked_jdo_transactions";
  public static final String JDO_COMMIT_TRANSACTIONS = "committed_jdo_transactions";
  public static final String JDO_OPEN_TRANSACTIONS = "opened_jdo_transactions";

  public static final String METASTORE_HIVE_LOCKS = "metastore_hive_locks";
  public static final String ZOOKEEPER_HIVE_SHAREDLOCKS = "zookeeper_hive_sharedlocks";
  public static final String ZOOKEEPER_HIVE_EXCLUSIVELOCKS = "zookeeper_hive_exclusivelocks";
  public static final String ZOOKEEPER_HIVE_SEMISHAREDLOCKS = "zookeeper_hive_semisharedlocks";

  public static final String EXEC_ASYNC_QUEUE_SIZE = "exec_async_queue_size";
  public static final String EXEC_ASYNC_POOL_SIZE = "exec_async_pool_size";

  public static final String OPERATION_PREFIX = "hs2_operation_";
  public static final String COMPLETED_OPERATION_PREFIX = "hs2_completed_operation_";

  public static final String SQL_OPERATION_PREFIX = "hs2_sql_operation_";
  public static final String COMPLETED_SQL_OPERATION_PREFIX = "hs2_completed_sql_operation_";

  public static final String INIT_TOTAL_DATABASES = "init_total_count_dbs";
  public static final String INIT_TOTAL_TABLES = "init_total_count_tables";
  public static final String INIT_TOTAL_PARTITIONS = "init_total_count_partitions";

  public static final String CREATE_TOTAL_DATABASES = "create_total_count_dbs";
  public static final String CREATE_TOTAL_TABLES = "create_total_count_tables";
  public static final String CREATE_TOTAL_PARTITIONS = "create_total_count_partitions";

  public static final String DELETE_TOTAL_DATABASES = "delete_total_count_dbs";
  public static final String DELETE_TOTAL_TABLES = "delete_total_count_tables";
  public static final String DELETE_TOTAL_PARTITIONS = "delete_total_count_partitions";

  public static final String DIRECTSQL_ERRORS = "directsql_errors";
}
