/*
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

import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Abstract base class for unit tests that wish to utilize randomness in their test strategies.
 *
 * @see "http://labs.carrotsearch.com/randomizedtesting.html"
 */
@Listeners(ReproducibleListener.class)
@RunWith(RandomizedRunner.class)
@ThreadLeakScope(ThreadLeakScope.Scope.TEST)
public abstract class RandomizedTestBase {

    public static Random random() {
        return RandomizedContext.current().getRandom();
    }

    public static boolean randomBoolean() {
        return random().nextBoolean();
    }

    public static byte randomByte() {
        return (byte) random().nextInt();
    }

    public static short randomShort() {
        return (short) random().nextInt();
    }

    public static int randomInt() {
        return random().nextInt();
    }

    public static float randomFloat() {
        return random().nextFloat();
    }

    public static double randomDouble() {
        return random().nextDouble();
    }

    public static long randomLong() {
        return random().nextLong();
    }

    public static int randomInt(int max) {
        return RandomizedTest.randomInt(max);
    }

    public static String randomAscii() {
        return RandomizedTest.randomAsciiOfLength(32);
    }

    public static String randomAsciiOfLength(int codeUnits) {
        return RandomizedTest.randomAsciiOfLength(codeUnits);
    }

    public static <T> T randomFrom(List<T> list) {
        return RandomPicks.randomFrom(random(), list);
    }

    public static <T> T randomFrom(T[] array) {
        return RandomPicks.randomFrom(random(), array);
    }

    public static <T> T randomFrom(Collection<T> collection) {
        return RandomPicks.randomFrom(random(), collection);
    }
}
