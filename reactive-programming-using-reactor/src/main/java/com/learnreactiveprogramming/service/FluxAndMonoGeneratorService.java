package com.learnreactiveprogramming.service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    public Flux<String> namesFluxMap() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .log();
    }

    public static void main(String[] args) {
        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();
        fluxAndMonoGeneratorService.namesFlux().subscribe(name -> System.out.println("Name is: " + name));
        fluxAndMonoGeneratorService.nameMono().subscribe(name -> System.out.println("Mono name is: " + name));

        fluxAndMonoGeneratorService.namesFluxMap().subscribe(name -> System.out.println("Name is: " + name));
    }
}
