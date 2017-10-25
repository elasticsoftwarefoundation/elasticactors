package org.elasticsoftware.elasticactors.eventsourcing;

import org.iban4j.CountryCode;
import org.iban4j.Iban;
import org.testng.annotations.Test;

public class IBANTest {
    @Test
    public void testRandomIban() {
        Iban iban = new Iban.Builder()
                .countryCode(CountryCode.NL)
                .bankCode("EAES")
                .buildRandom();
        System.out.println(iban.toFormattedString());
    }

}
