/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.service.metadata;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.proxy.service.route.MessageQueueView;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

public class ClusterMetadataServiceTest extends BaseServiceTest {

    private ClusterMetadataService clusterMetadataService;

    protected static final String BROKER2_ADDR = "127.0.0.2:10911";

    @Before
    public void before() throws Throwable {
        super.before();
        ConfigurationManager.getProxyConfig().setRocketMQClusterName(CLUSTER_NAME);

        TopicConfigAndQueueMapping topicConfigAndQueueMapping = new TopicConfigAndQueueMapping();
        topicConfigAndQueueMapping.setAttributes(new HashMap<>());
        topicConfigAndQueueMapping.setTopicMessageType(TopicMessageType.NORMAL);
        when(this.mqClientAPIExt.getTopicConfig(anyString(), eq(TOPIC), anyLong())).thenReturn(topicConfigAndQueueMapping);

        when(this.mqClientAPIExt.getSubscriptionGroupConfig(anyString(), eq(GROUP), anyLong())).thenReturn(new SubscriptionGroupConfig());

        this.clusterMetadataService = new ClusterMetadataService(this.topicRouteService, this.mqClientAPIFactory);

        BrokerData brokerData2 = new BrokerData();
        brokerData2.setBrokerName("brokerName2");
        HashMap<Long, String> addrs = new HashMap<>();
        addrs.put(MixAll.MASTER_ID, BROKER2_ADDR);
        brokerData2.setBrokerAddrs(addrs);
        brokerData2.setCluster(CLUSTER_NAME);
        topicRouteData.getBrokerDatas().add(brokerData2);
        when(this.topicRouteService.getAllMessageQueueView(any(), eq(TOPIC))).thenReturn(new MessageQueueView(CLUSTER_NAME, topicRouteData, null));

    }

    @Test
    public void testGetTopicMessageType() {
        ProxyContext ctx = ProxyContext.create();
        assertEquals(TopicMessageType.UNSPECIFIED, this.clusterMetadataService.getTopicMessageType(ctx, ERR_TOPIC));
        assertEquals(1, this.clusterMetadataService.topicConfigCache.asMap().size());
        assertEquals(TopicMessageType.UNSPECIFIED, this.clusterMetadataService.getTopicMessageType(ctx, ERR_TOPIC));

        assertEquals(TopicMessageType.NORMAL, this.clusterMetadataService.getTopicMessageType(ctx, TOPIC));
        assertEquals(2, this.clusterMetadataService.topicConfigCache.asMap().size());
    }

    @Test
    public void testGetSubscriptionGroupConfig() {
        ProxyContext ctx = ProxyContext.create();
        assertNotNull(this.clusterMetadataService.getSubscriptionGroupConfig(ctx, GROUP));
        assertEquals(1, this.clusterMetadataService.subscriptionGroupConfigCache.asMap().size());
    }

    @Test
    public void findOneBroker() {

        Set<String> resultBrokerNames = new HashSet<>();
        // run 1000 times to test the random
        for (int i = 0; i < 1000; i++) {
            Optional<BrokerData> brokerData = null;
            try {
                brokerData = this.clusterMetadataService.findOneBroker(TOPIC);
                resultBrokerNames.add(brokerData.get().getBrokerName());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        // we should choose two brokers
        assertEquals(2, resultBrokerNames.size());
    }
}
