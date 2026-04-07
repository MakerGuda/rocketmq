/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Broker 配置持久化 v2：基于 RocksDB 的配置与元数据管理实现包。
 * <p>
 * 与 v1 以 JSON 文件为主的 ConfigManager 相比，本包实现优先保证数据完整性与可靠性——RocksDB 写前日志常开并尽快刷盘；
 * 不再维护额外的堆内全量缓存，避免与 RocksDB MemTable/BlockCache 重复。
 * <p>
 * <strong>Endian</strong>：所有整型在网络与存储中均使用大端（网络字节序）。
 */
package org.apache.rocketmq.broker.config.v2;
