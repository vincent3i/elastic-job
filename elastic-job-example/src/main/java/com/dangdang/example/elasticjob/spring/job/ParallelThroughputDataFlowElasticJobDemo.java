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
package com.dangdang.example.elasticjob.spring.job;

import com.dangdang.ddframe.job.api.JobExecutionMultipleShardingContext;
import com.dangdang.ddframe.job.plugin.job.type.AbstractParallelThroughputDataFlowElasticJob;
import com.dangdang.example.elasticjob.fixture.entity.Foo;
import com.dangdang.example.elasticjob.fixture.repository.FooRepository;
import com.dangdang.example.elasticjob.utils.PrintContext;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component
public class ParallelThroughputDataFlowElasticJobDemo extends AbstractParallelThroughputDataFlowElasticJob<Foo> {

    private PrintContext printContext = new PrintContext(ParallelThroughputDataFlowElasticJobDemo.class);

    @Resource
    private FooRepository fooRepository;

    @Override
    public List<Foo> fetchData(JobExecutionMultipleShardingContext shardingContext) {
        System.out.println("shardingContext ==>> " + ReflectionToStringBuilder.toString(shardingContext));
        printContext.printFetchDataMessage(shardingContext.getShardingItems());
        return fooRepository.findActive(shardingContext.getShardingItems());
    }

    @Override
    public boolean processData(JobExecutionMultipleShardingContext shardingContext, Foo data) {
        printContext.printProcessDataMessage(data);
        if (9 == data.getId() % 10) {
            return false;
        }
        fooRepository.setInactive(data.getId());
        return true;
    }

    @Override
    public boolean isStreamingProcess() {
        return false;
    }
}
