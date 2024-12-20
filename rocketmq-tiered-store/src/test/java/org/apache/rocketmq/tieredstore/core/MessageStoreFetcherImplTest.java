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
package org.apache.rocketmq.tieredstore.core;

import com.google.common.collect.Sets;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.TieredMessageStore;
import org.apache.rocketmq.tieredstore.common.SelectBufferResult;
import org.apache.rocketmq.tieredstore.file.FlatMessageFile;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtilTest;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.tieredstore.core.MessageStoreFetcherImpl.CACHE_KEY_FORMAT;

public class MessageStoreFetcherImplTest {

    private String groupName;
    private MessageQueue mq;
    private MessageStoreConfig storeConfig;
    private TieredMessageStore messageStore;
    private MessageStoreDispatcherImplTest dispatcherTest;
    private MessageStoreFetcherImpl fetcher;

    @Before
    public void init() throws Exception {
        groupName = "GID-fetcherTest";
        dispatcherTest = new MessageStoreDispatcherImplTest();
        dispatcherTest.init();
    }

    @After
    public void shutdown() throws IOException {
        if (messageStore != null) {
            messageStore.destroy();
        }
        MessageStoreUtilTest.deleteStoreDirectory(dispatcherTest.storePath);
    }

    @Test
    public void getMessageFromTieredStoreTest() throws Exception {
        dispatcherTest.dispatchFromCommitLogTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        storeConfig.setReadAheadCacheEnable(true);
        fetcher = new MessageStoreFetcherImpl(messageStore);
        GetMessageResult getMessageResult = fetcher.getMessageAsync(
            groupName, mq.getTopic(), 0, 0, 32, null).join();
        Assert.assertEquals(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, getMessageResult.getStatus());

        getMessageResult = fetcher.getMessageAsync(
            groupName, mq.getTopic(), mq.getQueueId(), 0, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_TOO_SMALL, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(100L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageAsync(
            groupName, mq.getTopic(), mq.getQueueId(), 200, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_ONE, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageAsync(
            groupName, mq.getTopic(), mq.getQueueId(), 300, 32, null).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_BADLY, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        FlatMessageFile flatFile = dispatcherTest.fileStore.getFlatFile(mq);

        // direct
        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 0, 32).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_TOO_SMALL, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(100L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 200, 32).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_ONE, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 300, 32).join();
        Assert.assertEquals(GetMessageStatus.OFFSET_OVERFLOW_BADLY, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 100, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(100L + 32L, getMessageResult.getNextBeginOffset());

        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 180, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(20, getMessageResult.getMessageCount());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        // limit count or size
        int expect = 8;
        int size = getMessageResult.getMessageBufferList().get(0).remaining();
        storeConfig.setReadAheadMessageSizeThreshold(expect * size + 10);
        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 180, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(expect, getMessageResult.getMessageCount());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(180L + expect, getMessageResult.getNextBeginOffset());

        storeConfig.setReadAheadMessageCountThreshold(expect);
        storeConfig.setReadAheadMessageSizeThreshold(expect * size + expect * 2);
        getMessageResult = fetcher.getMessageFromTieredStoreAsync(flatFile, 180, 32).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(expect, getMessageResult.getMessageCount());
        Assert.assertEquals(100L, getMessageResult.getMinOffset());
        Assert.assertEquals(200L, getMessageResult.getMaxOffset());
        Assert.assertEquals(180L + expect, getMessageResult.getNextBeginOffset());
    }

    @Test
    public void getMessageFromCacheTest() throws Exception {
        this.getMessageFromTieredStoreTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        storeConfig.setReadAheadCacheEnable(true);
        storeConfig.setReadAheadMessageCountThreshold(32);
        storeConfig.setReadAheadMessageSizeThreshold(Integer.MAX_VALUE);

        int batchSize = 4;
        AtomicLong times = new AtomicLong(0L);
        AtomicLong offset = new AtomicLong(100L);
        FlatMessageFile flatFile = dispatcherTest.fileStore.getFlatFile(mq);
        Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
            GetMessageResult getMessageResult =
                fetcher.getMessageFromCacheAsync(flatFile, groupName, offset.get(), batchSize, null).join();
            offset.set(getMessageResult.getNextBeginOffset());
            times.incrementAndGet();
            return offset.get() == 200L;
        });
        Assert.assertEquals(100 / times.get(), batchSize);
    }

    @Test
    public void getMessageFromCacheTagFilterTest() throws Exception {
        dispatcherTest.dispatchFromCommitLogTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        storeConfig.setReadAheadCacheEnable(true);
        fetcher = new MessageStoreFetcherImpl(messageStore);

        FlatMessageFile flatFile = Mockito.mock(FlatMessageFile.class);
        Mockito.when(flatFile.getMessageQueue()).thenReturn(mq);
        Mockito.when(flatFile.getConsumeQueueMinOffset()).thenReturn(100L);
        Mockito.when(flatFile.getConsumeQueueMaxOffset()).thenReturn(200L);

        for (int i = 100; i < 200; i++) {
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            SelectBufferResult bufferResult = new SelectBufferResult(buffer, i, buffer.remaining(), i % 2);
            fetcher.getFetcherCache().put(
                String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), i), bufferResult);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setSubString("1 || 2");
        subscriptionData.setCodeSet(Sets.newHashSet(1, 2));
        MessageFilter filter = new DefaultMessageFilter(subscriptionData);

        GetMessageResult getMessageResult =
            fetcher.getMessageFromCacheAsync(flatFile, groupName, 100L, 32, filter).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(32, getMessageResult.getMessageCount());
        Assert.assertEquals(164L, getMessageResult.getNextBeginOffset());

        getMessageResult =
            fetcher.getMessageFromCacheAsync(flatFile, groupName, 164L, 32, filter).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(18, getMessageResult.getMessageCount());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        getMessageResult =
            fetcher.getMessageFromCacheAsync(flatFile, groupName, 200L, 32, filter).join();
        Assert.assertEquals(GetMessageStatus.NO_MATCHED_MESSAGE, getMessageResult.getStatus());
        Assert.assertEquals(200L, getMessageResult.getNextBeginOffset());

        subscriptionData.setCodeSet(Sets.newHashSet(0));
        filter = new DefaultMessageFilter(subscriptionData);
        getMessageResult =
            fetcher.getMessageFromCacheAsync(flatFile, groupName, 100L, 32, filter).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(32, getMessageResult.getMessageCount());
        Assert.assertEquals(164L - 1L, getMessageResult.getNextBeginOffset());
    }

    @Test
    public void getMessageFromCacheTagFilter2Test() throws Exception {
        dispatcherTest.dispatchFromCommitLogTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        storeConfig.setReadAheadCacheEnable(true);
        fetcher = new MessageStoreFetcherImpl(messageStore);

        FlatMessageFile flatFile = Mockito.mock(FlatMessageFile.class);
        Mockito.when(flatFile.getMessageQueue()).thenReturn(mq);
        Mockito.when(flatFile.getConsumeQueueMinOffset()).thenReturn(100L);
        Mockito.when(flatFile.getConsumeQueueMaxOffset()).thenReturn(200L);

        for (int i = 100; i < 200; i++) {
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            SelectBufferResult bufferResult = new SelectBufferResult(buffer, i, buffer.remaining(), i - 100L);
            fetcher.getFetcherCache().put(
                String.format(CACHE_KEY_FORMAT, mq.getTopic(), mq.getQueueId(), i), bufferResult);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setSubString("1 || 2");
        subscriptionData.setCodeSet(Sets.newHashSet(10, 20));
        MessageFilter filter = new DefaultMessageFilter(subscriptionData);

        GetMessageResult getMessageResult =
            fetcher.getMessageFromCacheAsync(flatFile, groupName, 100L, 2, filter).join();
        Assert.assertEquals(GetMessageStatus.FOUND, getMessageResult.getStatus());
        Assert.assertEquals(2, getMessageResult.getMessageCount());
        Assert.assertEquals(121L, getMessageResult.getNextBeginOffset());
    }

    @Test
    public void testGetMessageStoreTimeStampAsync() throws Exception {
        this.getMessageFromTieredStoreTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        long result1 = fetcher.getEarliestMessageTimeAsync(mq.getTopic(), 0).join();
        Assert.assertEquals(-1L, result1);

        long result2 = fetcher.getEarliestMessageTimeAsync(mq.getTopic(), mq.getQueueId()).join();
        Assert.assertEquals(11L, result2);

        long result3 = fetcher.getMessageStoreTimeStampAsync(mq.getTopic(), 0, 100).join();
        Assert.assertEquals(-1L, result3);

        long result4 = fetcher.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 100).join();
        Assert.assertEquals(11L, result4);

        long result5 = fetcher.getMessageStoreTimeStampAsync(mq.getTopic(), mq.getQueueId(), 120).join();
        Assert.assertEquals(11L, result5);
    }

    @Test
    public void testGetOffsetInQueueByTime() throws Exception {
        this.getMessageFromTieredStoreTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        // message time is all 11
        Assert.assertEquals(-1L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 0, 10, BoundaryType.LOWER));

        Assert.assertEquals(100L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 10, BoundaryType.LOWER));
        Assert.assertEquals(100L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 11, BoundaryType.LOWER));
        Assert.assertEquals(200L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 12, BoundaryType.LOWER));

        Assert.assertEquals(100L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 10, BoundaryType.UPPER));
        Assert.assertEquals(199L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 11, BoundaryType.UPPER));
        Assert.assertEquals(200L, fetcher.getOffsetInQueueByTime(mq.getTopic(), 1, 12, BoundaryType.UPPER));
    }

    @Test
    public void testQueryMessageAsync() throws Exception {
        this.getMessageFromTieredStoreTest();
        mq = dispatcherTest.mq;
        messageStore = dispatcherTest.messageStore;
        storeConfig = dispatcherTest.storeConfig;

        QueryMessageResult queryMessageResult = fetcher.queryMessageAsync(
            mq.getTopic(), "uk", 32, 0L, System.currentTimeMillis()).join();
        Assert.assertEquals(32, queryMessageResult.getMessageBufferList().size());

        queryMessageResult = fetcher.queryMessageAsync(
            mq.getTopic(), "uk", 120, 0L, System.currentTimeMillis()).join();
        Assert.assertEquals(100, queryMessageResult.getMessageBufferList().size());
    }
}
