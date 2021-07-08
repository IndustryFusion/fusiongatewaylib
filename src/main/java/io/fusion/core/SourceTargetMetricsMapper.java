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

import io.fusion.core.FusionDataServiceConfig.FieldSpec;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.stream.Collectors;

@Service
public class SourceTargetMetricsMapper implements MetricsMapper {
    private final FusionDataServiceConfig fusionDataServiceConfig;

    public SourceTargetMetricsMapper(FusionDataServiceConfig fusionDataServiceConfig) {
        this.fusionDataServiceConfig = fusionDataServiceConfig;
    }

    public Map<String, String> mapSourceToTargetMetrics(final String jobId, final Map<String, String> sourceMetrics) {
        var jobSpec = fusionDataServiceConfig.getJobSpecs().get(jobId);

        return jobSpec.getFields().stream()
                .filter(fieldSpec -> sourceMetrics.containsKey(fieldSpec.getKey()))
                .collect(Collectors.toMap(FieldSpec::getTarget, fieldSpec -> sourceMetrics.get(fieldSpec.getKey())));
    }
}