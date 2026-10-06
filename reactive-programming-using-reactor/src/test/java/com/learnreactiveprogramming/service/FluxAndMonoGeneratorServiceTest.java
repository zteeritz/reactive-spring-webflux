package com.learnreactiveprogramming.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;


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

    @Test
    void namesFluxFlatMap() {
        var namesFluxFlatMap = fluxAndMonoGeneratorService.namesFluxFlatMap(3);

        StepVerifier.create(namesFluxFlatMap)
                .expectNext("A")
                .expectNext("L")
                .expectNext("E")
                .expectNext("X")
                .expectNext("C")
                .expectNext("H")
                .expectNext("L")
                .expectNext("O")
                .expectNext("E")
                .verifyComplete();
    }

    @Test
    void namesFluxFlatMapAsync() {
        var namesFluxFlatMapAsync = fluxAndMonoGeneratorService.namesFluxFlatMapAsync(3);

        StepVerifier.create(namesFluxFlatMapAsync)
                .expectNextCount(9)
                .verifyComplete();
    }

    @Test
    void namesFluxConcatMap() {
        var namesFluxConcatMap = fluxAndMonoGeneratorService.namesFluxConcatMap(3);

        StepVerifier.create(namesFluxConcatMap)
                .expectNext("A")
                .expectNext("L")
                .expectNext("E")
                .expectNext("X")
                .expectNext("C")
                .expectNext("H")
                .expectNext("L")
                .expectNext("O")
                .expectNext("E")
                .verifyComplete();
    }

    @Test
    void namesMonoFlatMap() {
        var namesMonoFlatMap = fluxAndMonoGeneratorService.namesMonoFlatMap(2);

        StepVerifier.create(namesMonoFlatMap)
                .expectNext(List.of("A", "L", "E", "X"))
                .verifyComplete();
    }
}