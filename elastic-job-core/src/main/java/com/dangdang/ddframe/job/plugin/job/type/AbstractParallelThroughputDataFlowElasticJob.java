/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.dangdang.ddframe.job.plugin.job.type;

import com.dangdang.ddframe.job.api.JobExecutionMultipleShardingContext;
import com.dangdang.ddframe.job.internal.job.AbstractDataFlowElasticJob;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * 高吞吐量处理数据流程的作业.
 * @param <T> 数据流作业处理的数据实体类型
 * @author vincent
 */
@Slf4j
public abstract class AbstractParallelThroughputDataFlowElasticJob<T> extends AbstractDataFlowElasticJob<T, JobExecutionMultipleShardingContext> {

    private ForkJoinPool fjPool;

    private int THRESHOLD = 100;

    @Override
    protected void executeJob(JobExecutionMultipleShardingContext shardingContext) {
        if(isStreamingProcess()) {
            //流式处理fetchData方法的返回值只有为null或长度为空时，作业才会停止执行，否则作业会一直运行下去
            executeStreamingJob(shardingContext);
        } else {
            //如果不是流式处理则只会在每次作业执行过程中执行一次fetchData方法和processData方法，即完成本次作业
            executeOneOffJob(shardingContext);
        }
    }

    private void executeStreamingJob(final JobExecutionMultipleShardingContext shardingContext) {
        List<T> data = fetchDataWithLog(shardingContext);
        log.debug("Execute streaming job while data is not empty or task is running or does not need shard.");
        while (null != data && !data.isEmpty() && !isStoped() && !getShardingService().isNeedSharding()) {
            recursiveProcessData(shardingContext, data);
            data = fetchDataWithLog(shardingContext);
        }
    }

    private void executeOneOffJob(final JobExecutionMultipleShardingContext shardingContext) {
        List<T> data = fetchDataWithLog(shardingContext);
        log.debug("Execute streaming job while data is not empty.");
        if (null != data && !data.isEmpty()) {
            recursiveProcessData(shardingContext, data);
        }
    }

    private List<T> fetchDataWithLog(final JobExecutionMultipleShardingContext shardingContext) {
        List<T> result = fetchData(shardingContext);
        log.debug("Elastic job: fetch data size: {}.", result != null ? result.size() : 0);
        return result;
    }

    private void recursiveProcessData(final JobExecutionMultipleShardingContext shardingContext, final List<T> data) {
        int threadCount = getConfigService().getConcurrentDataProcessThreadCount();
        int availableProcessorNumber = Runtime.getRuntime().availableProcessors();
        int parallelism = Math.min(threadCount, availableProcessorNumber);
        log.debug("Parallelism is [{}] based on the minimum number between concurrent data process thread count [{}] and available processors number [{}]", parallelism, threadCount, availableProcessorNumber);
        this.fjPool = new ForkJoinPool(parallelism);

        //计算阈值，阈值大小为当前数量除以并发数乘以8得到的值，即并发量以当前处理能力的8倍为双端队列的大小
        int totalSize = data.size();
        parallelism *= 8;
        THRESHOLD = totalSize / parallelism;
        if (THRESHOLD == 0) {
            THRESHOLD = totalSize;
            log.info("Current size [{}] of data is less than eight time of eightfold parallelism [{}], use remainder [{}] as threshold.", totalSize, parallelism, THRESHOLD);
        }

        RecursiveProcessElasticDataFlowTask recursiveProcessElasticDataFlowTask = new RecursiveProcessElasticDataFlowTask(shardingContext, data, 0, totalSize, null);
        fjPool.invoke(recursiveProcessElasticDataFlowTask);
    }

    private class RecursiveProcessElasticDataFlowTask extends RecursiveAction {

        private final JobExecutionMultipleShardingContext shardingContext;

        private final List<T> data;

        private int low;

        private int high;

        /**
         * keeps track of right-hand-side tasks
         */
        private RecursiveProcessElasticDataFlowTask next;

        /**
         * 构造递归fork join
         * @param shardingContext 作业运行时多片分片上下文
         * @param data 待处理的数据
         * @param low 待处理的数据处理下标
         * @param high 待处理的数据上标，默认为待处理的数据的size
         * @param next 下一个递归fork信息，初始为null
         */
        private RecursiveProcessElasticDataFlowTask(JobExecutionMultipleShardingContext shardingContext,
                                                    List<T> data, int low, int high,
                                                    RecursiveProcessElasticDataFlowTask next) {
            this.shardingContext = shardingContext;
            this.data = data;
            this.next = next;

            this.low = low;
            this.high = high;
        }

        private void atLeaf(int low, int high) {
            log.info("low ==> {}, high ==> {}, length ==>{}", low, high, this.data.size());
            // perform leftmost base step
            for (int i = low; i < high; i++) {
                processDataWithStatistics(this.shardingContext, this.data.get(i));
            }
        }

        @Override
        protected void compute() {
            int low = this.low, high = this.high;
            //ThreadLocalRandom.current().nextInt();
            RecursiveProcessElasticDataFlowTask right = null;
            while (high - low > THRESHOLD && getSurplusQueuedTaskCount() <= 3) {
                //如果任务数量大于阈值，就分裂成两个任务
                int mid = (low + high) >>> 1;
                right = new RecursiveProcessElasticDataFlowTask(this.shardingContext, this.data, mid, high, right);
                right.fork();
                high = mid;
            }
            //如果任务粒度足够细则直接计算任务
            atLeaf(low, high);
            while (right != null) {
                if (right.tryUnfork()) // directly calculate if not stolen
                    right.atLeaf(right.low, right.high);
                else {
                    right.join();
                }
                right = right.next;
            }
        }
    }
}
