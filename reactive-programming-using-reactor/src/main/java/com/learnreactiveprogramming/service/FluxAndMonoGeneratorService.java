package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

public class FluxAndMonoGeneratorService {

    // Flux deals with multiple values
    public Flux<String> namesFlux() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .log(); // db or remote service call
    }

    // Mono deals with single value
    public Mono<String> nameMono() {
        return Mono.just("alex")
                .log();
    }

    public Flux<String> namesFluxMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .map(s -> s.length() + "-" + s)
                .log();
    }

    public Flux<String> namesFluxImmutable() {
        var namesFlux = Flux.fromIterable(List.of("alex", "ben", "chloe"));
        return namesFlux.map(String::toLowerCase);
    }

    public Flux<String> namesFluxFlatMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString) // A, L, E, X ...
                .log();
    }

    public Flux<String> namesFluxFlatMapAsync(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitStringAsync)
                .log();
    }

    public Flux<String> namesFluxConcatMap(int stringLength) {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .concatMap(this::splitStringAsync)
                .log();
    }

    public Flux<String> splitString(String name) {
        var charArray = name.split("");
        return Flux.fromArray(charArray);
    }

    public Flux<String> splitStringAsync(String name) {
        var charArray = name.split("");
        return Flux.fromArray(charArray).delayElements(Duration.ofMillis(100));
    }

    public Mono<List<String>> namesMonoFlatMap(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitStringMono);
    }

    private Mono<List<String>> splitStringMono(String s) {
        var charArray = s.split("");
        return Mono.just(List.of(charArray));
    }

    public Flux<String> namesMonoFlatMapMany(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMapMany(this::splitString)
                .log();
    }

    public Flux<String> namesFluxTransform(int stringLength) {
        Function<Flux<String>, Flux<String>> transformFunction = namesFlux -> namesFlux
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString);

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .transform(transformFunction)
                .defaultIfEmpty("default")
                .log();
    }

    public Flux<String> namesFluxTransformSwitchEmpty(int stringLength) {
        Function<Flux<String>, Flux<String>> transformFunction = namesFlux -> namesFlux
                .map(String::toUpperCase)
                .filter(name -> name.length() > stringLength)
                .flatMap(this::splitString);

        Flux<String> defaultFlux = Flux.just("default")
                .transform(transformFunction);

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .transform(transformFunction)
                .switchIfEmpty(defaultFlux)
                .log();
    }

    public Flux<String> exploreConcat() {
        return Flux.concat(nameMono(), namesFlux())
                .log();
    }

    public Flux<String> exploreConcatWith() {
        var aMono = Mono.just("a");
        var bMono = Mono.just("b");

        return aMono.concatWith(bMono).log();
    }

    public Flux<String> exploreMerge() {
        var abcFlux = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(100));
        var defFlux = Flux.just("D", "E", "F")
                .delayElements(Duration.ofMillis(125));
        return Flux.merge(abcFlux, defFlux).log();
    }

    public Flux<String> exploreMergeWithMono() {
        // publisher
        var aMono = Flux.just("A");
        // publisher
        var bMono = Flux.just("B");
        return aMono.mergeWith(bMono).log();
    }

    public Flux<String> exploreMergeSequential() {
        var abcFlux = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(100));
        var defFlux = Flux.just("D", "E", "F")
                .delayElements(Duration.ofMillis(125));
        return Flux.mergeSequential(abcFlux, defFlux).log();
    }

    public Flux<String> exploreZip() {
        var abcFlux = Flux.just("A", "B", "C");
        var defFlux = Flux.just("D", "E", "F");
        return Flux.zip(abcFlux, defFlux, (first, second) -> first + second);
    }

    public Flux<String> exploreZipTuple4() {
        var abcFlux = Flux.just("A", "B", "C");
        var defFlux = Flux.just("D", "E", "F");
        var _123Flux = Flux.just("1", "2", "3");
        var _456Flux = Flux.just("4", "5", "6");

        return Flux.zip(abcFlux, defFlux, _123Flux, _456Flux)
                .map(t4 -> t4.getT1() + t4.getT2() + t4.getT3() + t4.getT4())
                .log();
    }

    public Flux<String> exploreZipWith() {
        var abcFlux = Flux.just("A", "B", "C");
        var defFlux = Flux.just("D", "E", "F");
        return abcFlux.zipWith(defFlux, (first, second) -> first + second).log();
    }

    public Flux<String> exploreZipWithMono() {
        var abcFlux = Flux.just("A");
        var defMono = Mono.just("B");
        return abcFlux.zipWith(defMono, (first, second) -> first + second).log();
    }

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        fluxAndMonoGeneratorService.namesFlux()
                .subscribe(name -> System.out.println("Name is: " + name));
        fluxAndMonoGeneratorService.nameMono()
                .subscribe(name -> System.out.println("Mono name is: " + name));
        fluxAndMonoGeneratorService.namesFluxMap(2)
                .subscribe(name -> System.out.println("Name is: " + name));
        fluxAndMonoGeneratorService.namesFluxFlatMap(2)
                .subscribe(name -> System.out.println("Name is: " + name));
        fluxAndMonoGeneratorService.namesFluxFlatMapAsync(2)
                .subscribe(name -> System.out.println("Name is: " + name));
        fluxAndMonoGeneratorService.namesMonoFlatMap(2)
                .subscribe(name -> System.out.println("Name is: " + name));
    }
}
