/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.etl.api;

import java.util.Map;

public interface RealTimePipeline
{
    /**
     * 初始化(driver阶段执行) 需要注意序列化问题
     */
    default void driverInit(Map<String, Object> optionMap) {}

    /**
     * partition级别的初始化
     **/
    boolean open(long partitionId, long version);

    /**
     * partition级别的资源释放
     **/
    void close(Throwable errorOrNull);
}
