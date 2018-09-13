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
package ideal.sylph.runner.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.common.classloader.ThreadContextClassLoader;
import ideal.common.proxy.DynamicProxy;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.runner.spark.yarn.SparkAppLauncher;
import ideal.sylph.runner.spark.yarn.YarnJobContainer;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.EtlFlow;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobConfig;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.model.PipelinePluginManager;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;

@Name("Spark_Structured_StreamETL")
@Description("spark2.x Structured streaming StreamETL")
public class Stream2EtlActuator
        implements JobActuatorHandle
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Inject private YarnClient yarnClient;
    @Inject private SparkAppLauncher appLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @Nullable
    @Override
    public Collection<File> parserFlowDepends(Flow inFlow)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;
        //---- flow parser depends ----
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        for (NodeInfo nodeInfo : flow.getNodes()) {
            String json = JsonTextUtil.readJsonText(nodeInfo.getNodeText());
            Map<String, Object> nodeConfig = nodeInfo.getNodeConfig();
            Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
            String driverString = (String) requireNonNull(config.get("driver"), "driver is null");
            Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = pluginManager.findPluginInfo(driverString);
            pluginInfo.ifPresent(plugin -> FileUtils.listFiles(plugin.getPluginFile(), null, true)
                    .forEach(builder::add));
        }
        return builder.build();
    }

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, JobConfig jobConfig, URLClassLoader jobClassLoader)
            throws IOException
    {
        try {
            return JobHelper.build2xJob(jobId, (EtlFlow) inFlow, jobClassLoader, pluginManager);
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }

    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        final JobContainer yarnJobContainer = new YarnJobContainer(yarnClient, jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = appLauncher.run(job);
                this.setYarnAppId(yarnAppId);
                return Optional.of(yarnAppId.toString());
            }
        };
        //----create JobContainer Proxy
        DynamicProxy invocationHandler = new DynamicProxy(yarnJobContainer)
        {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable
            {
                /*
                 * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                 * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                 * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                 * */
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
                    return method.invoke(yarnJobContainer, args);
                }
            }
        };

        return (JobContainer) invocationHandler.getProxy(JobContainer.class);
    }
}
