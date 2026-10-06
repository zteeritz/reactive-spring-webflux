package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

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
