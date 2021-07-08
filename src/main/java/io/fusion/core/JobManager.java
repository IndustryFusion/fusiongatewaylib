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

package io.fusion.core;

import io.fusion.core.FusionDataServiceConfig.DataServiceType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Component
@Slf4j
public class JobManager {
    private final TaskScheduler scheduler;
    private final FusionDataServiceConfig config;
    private final MetricsMapper metricsMapper;
    private final MetricsPullService metricsPullService;
    private final OutputService outputService;
    private List<Future<?>> jobs;

    public JobManager(TaskScheduler scheduler, FusionDataServiceConfig config, MetricsMapper metricsMapper,
                      MetricsPullService metricsPullService, OutputService outputService) {
        this.scheduler = scheduler;
        this.config = config;
        this.metricsPullService = metricsPullService;
        this.metricsMapper = metricsMapper;
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
        if (config.getDataServiceType().equals(DataServiceType.PULL)) {
            jobs = config.getJobSpecs().entrySet().stream()
                    .map(mapEntry -> scheduler.scheduleAtFixedRate(
                            new PullMetricsAndOutputJob(mapEntry.getKey(),
                                    metricsPullService,
                                    outputService,
                                    metricsMapper),
                            mapEntry.getValue().getPeriod()))
                    .collect(Collectors.toList());
        }
    }

    public void cancel() {
        if (jobs != null) {
            jobs.forEach(future -> future.cancel(true));
        }
    }
}
