/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.fusion.core.job;

import io.fusion.core.config.FusionDataServiceConfig;
import io.fusion.core.config.FusionDataServiceConfig.DataServiceType;
import io.fusion.core.mapper.MetricsMapper;
import io.fusion.core.output.OutputService;
import io.fusion.core.source.MetricsPullService;
import io.fusion.core.source.MetricsPushService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Component
@Slf4j
public class JobManager {
    private final ThreadPoolTaskScheduler taskScheduler;
    private final FusionDataServiceConfig config;
    private final MetricsMapper metricsMapper;
    private final MetricsPullService metricsPullService;
    private final MetricsPushService metricsPushService;
    private final OutputService outputService;
    private List<Future<?>> pullJobs;
    private List<PushMetricsAndOutputJob> pushJobs;

    public JobManager(ThreadPoolTaskScheduler taskScheduler, FusionDataServiceConfig config,
                      MetricsMapper metricsMapper, MetricsPullService metricsPullService,
                      MetricsPushService metricsPushService, OutputService outputService) {
        this.taskScheduler = taskScheduler;
        this.config = config;
        this.metricsPullService = metricsPullService;
        this.metricsMapper = metricsMapper;
        this.metricsPushService = metricsPushService;
        this.outputService = outputService;
    }

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (config.getAutorun() == null || config.getAutorun()) {
            start();
        }
    }

    public void start() {
        log.info("Starting JobManager");
        if (config.getDataServiceType() != null && config.getDataServiceType().equals(DataServiceType.PULL)) {
            pullJobs = config.getJobSpecs().entrySet().stream()
                    .map(mapEntry -> taskScheduler.scheduleWithFixedDelay(
                            new PullMetricsAndOutputJob(mapEntry.getKey(),
                                    metricsPullService,
                                    outputService,
                                    metricsMapper),
                            mapEntry.getValue().getPeriod()))
                    .collect(Collectors.toList());
        }
        if (config.getDataServiceType() != null && config.getDataServiceType().equals(DataServiceType.PUSH)) {
            pushJobs = config.getJobSpecs().keySet().stream()
                    .map(jobId -> new PushMetricsAndOutputJob(jobId,
                                    metricsPushService,
                                    outputService,
                                    metricsMapper))
                    .collect(Collectors.toList());
            pushJobs.forEach(PushMetricsAndOutputJob::start);
        }
    }

    public void cancel() {
        if (pullJobs != null) {
            pullJobs.forEach(future -> future.cancel(true));
        }
        if (pushJobs != null) {
            pushJobs.forEach(PushMetricsAndOutputJob::stop);
        }
    }

    public boolean isDone() {
        if (pullJobs != null) {
            return pullJobs.stream().map(Future::isDone).reduce(Boolean.TRUE, Boolean::logicalAnd);
        }
        return true;
    }
}
