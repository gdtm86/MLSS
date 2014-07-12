/*
 * Copyright 2013 Microsoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.examples.nggroup.bgd.utils;

import org.mortbay.log.Log;

import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.task.TaskConfigurationOptions;
import com.microsoft.reef.examples.nggroup.bgd.MasterTask;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.ConfigurationSerializer;

/**
 *
 */
public class SubConfiguration {

  @SafeVarargs
  public static Configuration from(final Configuration baseConf, final Class<? extends Name<?>>... classes) {
    final Injector injector = Tang.Factory.getTang().newInjector(baseConf);
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Class<? extends Name<?>> clazz : classes) {
      try {
        confBuilder
                .bindNamedParameter(clazz, injector.getNamedInstance((Class<? extends Name<Object>>) clazz).toString());
      } catch (BindException | InjectionException e) {
        final String msg = "Exception while creating subconfiguration";
        Log.warn(msg);
        throw new RuntimeException(msg, e);
      }
    }
    return confBuilder.build();
  }

  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration conf = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "TASK")
        .set(TaskConfiguration.TASK, MasterTask.class)
        .build();
    final ConfigurationSerializer confSerizalizer = new AvroConfigurationSerializer();
    System.out.println("Base conf:");
    System.out.println(confSerizalizer.toString(conf));
    final Configuration subConf = SubConfiguration.from(conf, TaskConfigurationOptions.Identifier.class);
    System.out.println("Sub conf:");
    System.out.println(confSerizalizer.toString(subConf));
  }

}
