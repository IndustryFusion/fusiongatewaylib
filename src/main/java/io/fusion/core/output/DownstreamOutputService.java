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

package io.fusion.core.output;

import io.fusion.core.config.FusionDataServiceConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@Slf4j
public class DownstreamOutputService implements OutputService {
    private final FusionDataServiceConfig fusionDataServiceConfig;

    public DownstreamOutputService(FusionDataServiceConfig fusionDataServiceConfig) {
        this.fusionDataServiceConfig = fusionDataServiceConfig;
    }

    private static String ensureTrailingSlash(final String url) {
        if (!url.endsWith("/")) {
            return url + "/";
        }
        return url;
    }

    @Override
    public void sendMetrics(final String jobId, Map<String, Object> metrics) {
        var restTemplate = new RestTemplate();
        var url = getRestServiceUrl(jobId);
        ResponseEntity<String> response = restTemplate.postForEntity(url, metrics, String.class);
        log.info("Sent {} metrics to {} for job {}", metrics.size(), url, jobId);
        if (!response.getStatusCode().is2xxSuccessful()) {
            log.warn("Post metrics not successful: {} ({})", response.getStatusCode(), response.getBody());
        }
    }

    private String getRestServiceUrl(final String jobId) {
        return ensureTrailingSlash(fusionDataServiceConfig.getDownstreamServiceBaseUrl()) + jobId;
    }
}
