package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;


class FluxAndMonoGeneratorServiceTest {

    FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

    @Test
    void namesFlux() {

        var namesFlux = fluxAndMonoGeneratorService.namesFlux();

        StepVerifier.create(namesFlux)
//                .expectNext("alex", "ben", "chloe")
//                .expectNextCount(3)
                .expectNext("alex")
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void namesFluxMap() {

        int stringLength = 2;

        var namesFluxMap = fluxAndMonoGeneratorService.namesFluxMap(stringLength);

        StepVerifier.create(namesFluxMap)
                .expectNext("4-ALEX")
                .expectNext("3-BEN")
                .expectNext("5-CHLOE")
                .verifyComplete();
    }

    @Test
    void namesFluxImmutable() {
        var namesFluxImmutable = fluxAndMonoGeneratorService.namesFluxImmutable();

        StepVerifier.create(namesFluxImmutable)
                .expectNext("alex")
                .expectNext("ben")
                .expectNext("chloe")
                .verifyComplete();
    }
}