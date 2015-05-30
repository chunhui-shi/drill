/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.elasticsearch.random;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.ReproduceErrorMessageBuilder;

/**
 *
 */
public class ReproducibleListener extends RunListener {

    @Override
    public void testStarted(Description description) throws Exception {
        // If you find it useful, add something here
    }

    @Override
    public void testFinished(Description description) throws Exception {
        // If you find it useful, add something here
    }

    @Override
    public void testFailure(Failure failure) throws Exception {

        StringBuilder sb = new StringBuilder();

        sb.append("\n==============================================================\n");
        sb.append("Command line to reproduce test failure with the same random seed:\n\tmvn test");
        ReproduceErrorMessageBuilder mavenMessageBuilder = new ReproduceErrorMessageBuilder(sb);
        mavenMessageBuilder.appendAllOpts(failure.getDescription());
        sb.append("\n==============================================================\n");

        System.err.println(sb.toString());
    }
}
