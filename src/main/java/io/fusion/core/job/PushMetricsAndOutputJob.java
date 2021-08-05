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

import io.fusion.core.mapper.MetricsMapper;
import io.fusion.core.output.OutputService;
import io.fusion.core.source.MetricsPushService;
import io.fusion.core.source.PushCallback;

import java.util.Map;

public class PushMetricsAndOutputJob implements PushCallback {
    private final String jobId;
    private final MetricsPushService metricsPushService;
    private final OutputService outputService;
    private final MetricsMapper metricsMapper;

    public PushMetricsAndOutputJob(String jobId, MetricsPushService metricsPushService, OutputService outputService,
                                   MetricsMapper metricsMapper) {
        this.jobId = jobId;
        this.metricsPushService = metricsPushService;
        this.outputService = outputService;
        this.metricsMapper = metricsMapper;
    }

    public void start() {
        this.metricsPushService.start(jobId, this);
    }

    public void stop() {
        this.metricsPushService.stop(jobId);
    }

    @Override
    public void handleMetrics(String jobId, Map<String, Object> metrics) {
        Map<String, Object> targetMetrics = metricsMapper.mapSourceToTargetMetrics(jobId, metrics);

        outputService.sendMetrics(jobId, targetMetrics);
    }
}
