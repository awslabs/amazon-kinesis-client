/*
 * Copyright 2024 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.kinesis.worker.platform;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import lombok.RequiredArgsConstructor;
import software.amazon.kinesis.annotations.KinesisClientInternalApi;

/**
 * Utility class to open a URL and get the input stream.
 */
@RequiredArgsConstructor
@KinesisClientInternalApi
class UrlOpener {
    private final URL url;

    /**
     * Open the URL and return the connection.
     *
     * @return a HttpURLConnection.
     * @throws IOException if a connection cannot be established.
     */
    public HttpURLConnection openConnection() throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    /**
     * Get the input stream from the connection.
     *
     * @param connection the connection to get the input stream from.
     * @return the InputStream for the data.
     * @throws IOException if an error occurs while getting the input stream.
     */
    public InputStream getInputStream(HttpURLConnection connection) throws IOException {
        return connection.getInputStream();
    }
}
