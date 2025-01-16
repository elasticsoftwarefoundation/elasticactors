/*
 * Copyright 2013 - 2025 The Original Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package org.elasticsoftware.elasticactors.tracing;

import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import static org.elasticsoftware.elasticactors.tracing.TracingUtils.shorten;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TracingUtilsTest {

    @Test
    public void testNextTraceIdHigh() {
        // given
        Random rand = mock(Random.class);
        given(rand.nextInt()).willReturn(0x3edfadad);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1601756625000L), ZoneId.systemDefault());

        // when
        String nextTraceIdHigh = TracingUtils.nextTraceIdHigh(clock, rand);

        // then
        assertEquals(nextTraceIdHigh, "5f78ddd13edfadad");
    }

    @Test
    public void testNextTraceIdHigh_shouldPadRandomPart() {
        // given
        Random rand = mock(Random.class);
        given(rand.nextInt()).willReturn(0xedfadad);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1601756625000L), ZoneId.systemDefault());

        // when
        String nextTraceIdHigh = TracingUtils.nextTraceIdHigh(clock, rand);

        // then
        assertEquals(nextTraceIdHigh, "5f78ddd10edfadad");
    }

    @Test
    public void testNextTraceIdHigh_shouldPadTimePart() {
        // given
        Random rand = mock(Random.class);
        given(rand.nextInt()).willReturn(0x3edfadad);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1756625000L), ZoneId.systemDefault());

        // when
        String nextTraceIdHigh = TracingUtils.nextTraceIdHigh(clock, rand);

        // then
        assertEquals(nextTraceIdHigh, "001acdd13edfadad");
    }

    @Test
    public void testNextTraceIdHigh_shouldPadTimeAndRandomParts() {
        // given
        Random rand = mock(Random.class);
        given(rand.nextInt()).willReturn(0xedfadad);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1756625000L), ZoneId.systemDefault());

        // when
        String nextTraceIdHigh = TracingUtils.nextTraceIdHigh(clock, rand);

        // then
        assertEquals(nextTraceIdHigh, "001acdd10edfadad");
    }

    @Test
    public void testToHexString() {
        // given
        long[] numbers = {
            0x0L,
            0x1L,
            0x10L,
            0x100L,
            0x1000L,
            0x10000L,
            0x100000L,
            0x1000000L,
            0x10000000L,
            0x100000000L,
            0x1000000000L,
            0x10000000000L,
            0x100000000000L,
            0x1000000000000L,
            0x10000000000000L,
            0x100000000000000L,
            0x1000000000000000L
        };

        // when
        String[] hexStrings = Arrays.stream(numbers)
            .mapToObj(TracingUtils::toHexString)
            .toArray(String[]::new);

        // then
        assertEquals(hexStrings, new String[]{
            "0000000000000000",
            "0000000000000001",
            "0000000000000010",
            "0000000000000100",
            "0000000000001000",
            "0000000000010000",
            "0000000000100000",
            "0000000001000000",
            "0000000010000000",
            "0000000100000000",
            "0000001000000000",
            "0000010000000000",
            "0000100000000000",
            "0001000000000000",
            "0010000000000000",
            "0100000000000000",
            "1000000000000000"
        });
    }

    @Test
    public void testShorten() {
        assertNull(shorten((String) null));
        assertEquals(shorten(""), "");
        assertEquals(shorten("Some unshortenable string!"), "Some unshortenable string!");
        assertEquals(
            shorten("org.elasticsoftware.elasticactors.ElasticActorsClass"),
            "o.e.e.ElasticActorsClass"
        );
        assertEquals(
            shorten("org.elasticsoftware.elasticactors.ElasticActorsClass[]"),
            "o.e.e.ElasticActorsClass[]"
        );
        assertEquals(
            shorten("org.elasticsoftware.elasticactors.ElasticActorsClass[][]"),
            "o.e.e.ElasticActorsClass[][]"
        );
    }

    @Test
    public void testTestShortenClass() {
        assertNull(shorten((Class<?>) null));
        assertEquals(shorten(byte.class), "byte");
        assertEquals(shorten(int[].class), "int[]");
        assertEquals(shorten(Object[].class), "j.l.Object[]");
        assertEquals(shorten(Object[][].class), "j.l.Object[][]");
        assertEquals(shorten(String.class), "j.l.String");
        assertEquals(shorten(getClass()), "o.e.e.t.TracingUtilsTest");
    }

    @Test
    public void testTestShortenClassList() {
        assertEquals(
            shorten(new Class[]{
                String.class,
                Object.class,
                BigDecimal.class,
                Short[].class,
                int.class,
                long[].class,
                Object[][].class
            }),
            "j.l.String,j.l.Object,j.m.BigDecimal,j.l.Short[],int,long[],j.l.Object[][]"
        );
    }

    @Test
    public void testTestShortenMethod() throws NoSuchMethodException {
        assertEquals(
            shorten(String.class.getMethod("codePointCount", int.class, int.class)),
            "j.l.String.codePointCount(int,int)"
        );
        assertEquals(
            shorten(String.class.getMethod("format", Locale.class, String.class, Object[].class)),
            "j.l.String.format(j.u.Locale,j.l.String,j.l.Object[])"
        );
    }
}