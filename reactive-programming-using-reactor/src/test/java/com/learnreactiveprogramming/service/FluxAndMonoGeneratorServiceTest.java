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

    @Test
    void namesMonoFlatMapMany() {
        var namesMonoFlatMapMany = fluxAndMonoGeneratorService.namesMonoFlatMapMany(2);

        StepVerifier.create(namesMonoFlatMapMany)
                .expectNext("A")
                .expectNext("L")
                .expectNext("E")
                .expectNext("X")
                .verifyComplete();
    }

    @Test
    void namesFluxTransform() {
        var namesFluxTransform = fluxAndMonoGeneratorService.namesFluxTransform(2);

        StepVerifier.create(namesFluxTransform)
                .expectNext("A")
                .expectNext("L")
                .expectNext("E")
                .expectNext("X")
                .expectNext("B")
                .expectNext("E")
                .expectNext("N")
                .expectNext("C")
                .expectNext("H")
                .expectNext("L")
                .expectNext("O")
                .expectNext("E")
                .verifyComplete();
    }

    @Test
    void namesTransformDefault() {
        var namesFluxTransform = fluxAndMonoGeneratorService.namesFluxTransform(10);

        StepVerifier.create(namesFluxTransform)
                .expectNext("default")
                .verifyComplete();
    }

    @Test
    void namesTransformSwitchIfEmpty() {
        var namesFluxTransform = fluxAndMonoGeneratorService
                .namesFluxTransformSwitchEmpty(6);

        StepVerifier.create(namesFluxTransform)
                .expectNext("D", "E", "F", "A", "U", "L", "T")
                .verifyComplete();
    }

    @Test
    void exploreConcat() {
        var namesConcat = fluxAndMonoGeneratorService.exploreConcat();
        StepVerifier.create(namesConcat)
                .expectNext("alex")
                .expectNext("alex")
                .expectNext("ben")
                .expectNext("chloe")
                .verifyComplete();
    }

    @Test
    void exploreMerge() {
        var value = fluxAndMonoGeneratorService.exploreMerge();
        StepVerifier.create(value)
                .expectNext("A", "D", "B", "E", "C", "F")
                .verifyComplete();
    }
}