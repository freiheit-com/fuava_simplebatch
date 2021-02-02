/*
 * Copyright 2015 freiheit.com technologies gmbh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.freiheit.fuava.simplebatch.fetch;

import com.freiheit.fuava.simplebatch.result.Result;

public interface PageFetcher<T> {
    public static final class PagingInput {
        public final int from;
        public final int pageSize;

        public PagingInput( final int from, final int pageSize ) {
            this.from = from;
            this.pageSize = pageSize;
        }
    }

    public Result<PagingInput, T> fetch(
            int from, int pageSize
            );
}
