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

import java.util.Map;

public class PullMetricsAndOutputJob implements Runnable {
    private final String jobId;
    private final MetricsPullService metricsPullService;
    private final OutputService outputService;
    private final MetricsMapper metricsMapper;

    public PullMetricsAndOutputJob(String jobId,
                                   MetricsPullService metricsPullService,
                                   OutputService outputService,
                                   MetricsMapper metricsMapper) {
        this.jobId = jobId;
        this.metricsPullService = metricsPullService;
        this.outputService = outputService;
        this.metricsMapper = metricsMapper;
    }

    @Override
    public void run() {
        Map<String, String> sourceMetrics = metricsPullService.getMetrics(jobId);
        Map<String, String> targetMetrics = metricsMapper.mapSourceToTargetMetrics(jobId, sourceMetrics);

        outputService.sendMetrics(jobId, targetMetrics);
    }
}
