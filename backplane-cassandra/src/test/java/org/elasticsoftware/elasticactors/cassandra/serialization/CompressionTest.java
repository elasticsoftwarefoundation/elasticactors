/*
 *   Copyright 2013 - 2019 The Original Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.elasticsoftware.elasticactors.cassandra.serialization;

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Joost van de Wijgerd
 */
public class CompressionTest {
    @Test
    public void testNoCompression() throws IOException {
        String testString = "This string is too small and should not be compressed";

        CompressingSerializer<String> serializer = new CompressingSerializer<>(new StringSerializer());
        byte[] serialized = serializer.serialize(testString);
        assertEquals(new String(serialized, StandardCharsets.UTF_8), testString);

        DecompressingDeserializer<String> deserializer = new DecompressingDeserializer<>(new StringDeserializer());
        String deserializedString = deserializer.deserialize(serialized);

        assertEquals(deserializedString,testString);

    }

    @Test
    public void testCompression() throws IOException {
        String testString = "OK. Let’s take a step back from all the recent tech news, look at it with fresh eyes — and try not to burst into slightly hysterical laughter.\n" +
                "\n" +
                "In Japan, some half-billion dollars’ worth of cryptocurrency vanished from a site founded to trade Magic: The Gathering cards. In New Zealand, the world’s greatest Call of Duty player has launched a political party to revenge himself on those who had him arrested and seized his sports cars. In Britain, the secret service is busy collecting and watching homegrown porn.\n" +
                "\n" +
                "Here in Silicon Valley, mighty Apple just revealed that a flagrant, basic programming error gutted the security of all its devices for years. Google, “more wood behind fewer arrows” Google, now has its own navy, to go with its air force and robot army. Meanwhile, down Sand Hill Road, venture capitalists are gravely concerned that the tech startups they’re investing in just aren’t crazy enough:\n" +
                "\n" +
                "Thiel Venn diagram\n" +
                "\n" +
                "Wait, it gets weirder. The Jamaican Olympic bobsled team was partly funded by $30,000 worth of dogecoin. TechCrunch’s Kim-Mai Cutler is crowdfunding an investigative journalism venture to Vietnam to find out why the viral breakout hit “Flappy Bird” was pulled from the App Store; the venture is already oversubscribed. Oh yeah. And then there’s Shingy.\n" +
                "\n" +
                "I’m visiting my parents in Florida later this month, and am kind of dreading any discussions of the tech industry. I fear it might go a bit like this:\n" +
                "\n" +
                "\n" +
                "The notion that Mt.Gox lost hundreds of thousands of bitcoins over a period of years without noticing is completely implausible verging on insane — but still less insane than Mt.Gox’s one-time reign as Bitcoin Central in the first place. And let’s face it, Bitcoin itself is pretty absurdist, and I say that as a long-term believer in cryptocurrencies.\n" +
                "\n" +
                "\n" +
                "In case it isn’t obvious, I think all this neo-high-tech-Dadaism is awesome. I mean, it’s not all good. Some of it is bad, if not outright evil. But it remains awesome. Take, for instance, the NSA’s darkly hilarious profusion of data-gathering programs. As Bruce Schneier puts it:\n" +
                "\n" +
                "I can name three different NSA programs to collect Gmail user data. These programs are based on three different technical eavesdropping capabilities. They rely on three different legal authorities. They involve collaborations with three different companies. And this is just Gmail. The same is true for cell phone call records, Internet chats, cell-phone location data.\n" +
                "\n" +
                "Schneier views this as deliberate obfuscation; I think it’s just a side effect of the fact that the evil NSA bureaucrats are still fundamentally bureaucrats, obsessed with subdividing everything into separate silos and processes (with a bizarre penchant for WTF names.) It’s as if Office Space‘s Milton made a Faustian deal with the devil –";

        CompressingSerializer<String> serializer = new CompressingSerializer<>(new StringSerializer());
        byte[] serialized = serializer.serialize(testString);

        assertTrue(serialized.length < testString.getBytes(StandardCharsets.UTF_8).length);

        DecompressingDeserializer<String> deserializer = new DecompressingDeserializer<>(new StringDeserializer());
        String deserializedString = deserializer.deserialize(serialized);

        assertEquals(deserializedString,testString);
    }

    @Test
    public void testCompressionJSON() throws IOException {
        String testString = "[{\"symbol\":\"Imtech\",\"securityId\":\"24154\",\"quoteCurrency\":\"EUR\",\"currentPrice\":\"2.00\"},{\"symbol\":\"Facebook\",\"securityId\":\"21977\",\"quoteCurrency\":\"USD\",\"currentPrice\":\"68.34\"},{\"symbol\":\"Twitter\",\"securityId\":\"25607\",\"quoteCurrency\":\"USD\",\"currentPrice\":\"54.97\"},{\"symbol\":\"Apple\",\"securityId\":\"12749\",\"quoteCurrency\":\"USD\",\"currentPrice\":\"526.99\"},{\"symbol\":\"Google\",\"securityId\":\"12746\",\"quoteCurrency\":\"USD\",\"currentPrice\":\"1216.40\"},{\"symbol\":\"Tesla\",\"securityId\":\"24348\",\"quoteCurrency\":\"USD\",\"currentPrice\":\"245.06\"},{\"symbol\":\"Vodaphone\",\"securityId\":\"12917\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"249.00\"},{\"symbol\":\"HSBC\",\"securityId\":\"12932\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"629.20\"},{\"symbol\":\"Tesco\",\"securityId\":\"12761\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"329.08\"},{\"symbol\":\"EUR/GBP\",\"securityId\":\"12956\",\"quoteCurrency\":\"FGB\",\"currentPrice\":\"0.82432\"},{\"symbol\":\"Silver\",\"securityId\":\"20680\",\"quoteCurrency\":\"CUS\",\"currentPrice\":\"21.248\"},{\"symbol\":\"EUR/USD\",\"securityId\":\"26484\",\"quoteCurrency\":\"FUS\",\"displayDecimals\":5,\"maxLeverage\":50,\"multiplier\":10,\"currentPrice\":\"1.38021\"}]";

        CompressingSerializer<String> serializer = new CompressingSerializer<>(new StringSerializer(),1024);
        byte[] serialized = serializer.serialize(testString);

        assertTrue(serialized.length < testString.getBytes(StandardCharsets.UTF_8).length);

        DecompressingDeserializer<String> deserializer = new DecompressingDeserializer<>(new StringDeserializer());
        String deserializedString = deserializer.deserialize(serialized);

        assertEquals(deserializedString,testString);

    }

    @Test
    public void testCompressionJSONSmall() throws IOException {
        String testString = "[{\"symbol\":\"Vodaphone\",\"securityId\":\"12917\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"249.00\"},{\"symbol\":\"HSBC\",\"securityId\":\"12932\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"629.20\"},{\"symbol\":\"Tesco\",\"securityId\":\"12761\",\"quoteCurrency\":\"GBP\",\"currentPrice\":\"329.08\"},{\"symbol\":\"EUR/GBP\",\"securityId\":\"12956\",\"quoteCurrency\":\"FGB\",\"currentPrice\":\"0.82432\"},{\"symbol\":\"Silver\",\"securityId\":\"20680\",\"quoteCurrency\":\"CUS\",\"currentPrice\":\"21.248\"},{\"symbol\":\"EUR/USD\",\"securityId\":\"26484\",\"quoteCurrency\":\"FUS\",\"displayDecimals\":5,\"maxLeverage\":50,\"multiplier\":10,\"currentPrice\":\"1.38021\"}]";

        CompressingSerializer<String> serializer = new CompressingSerializer<>(new StringSerializer(),512);
        byte[] serialized = serializer.serialize(testString);

        assertTrue(serialized.length < testString.getBytes(StandardCharsets.UTF_8).length);

        DecompressingDeserializer<String> deserializer = new DecompressingDeserializer<>(new StringDeserializer());
        String deserializedString = deserializer.deserialize(serialized);

        assertEquals(deserializedString,testString);

    }
}
