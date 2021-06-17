package com.pk.bigprime;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.Attributes;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class BigPrimeExample {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "ActorSystem");

        Source<Integer, NotUsed> intSource = Source.range(1, 10);
        Flow<Integer, BigInteger, NotUsed> bigIntFlow = Flow.of(Integer.class).map(i -> {
            BigInteger value = new BigInteger(3000, new Random());
            actorSystem.log().info("Big integer: " + value);
            return value;
        });
        Flow<BigInteger, BigInteger, NotUsed> bigPrimeFlow = Flow.of(BigInteger.class).map(i -> {
            BigInteger value = i.nextProbablePrime();
            actorSystem.log().info("******Prime: " + value);
            return value;
        });

        Flow<BigInteger, BigInteger, NotUsed> bigPrimeFlowAsync = Flow.of(BigInteger.class).mapAsyncUnordered(4, i -> {
            CompletableFuture<BigInteger> futurePrime = new CompletableFuture<>();
            futurePrime.completeAsync(() -> {
                BigInteger value = i.nextProbablePrime();
                actorSystem.log().info("******Prime: " + value);
                return value;
            });
            return futurePrime;
        });

        Flow<BigInteger, List<BigInteger>, NotUsed> listFLow = Flow.of(BigInteger.class)
                                                                    .grouped(10)
                                                                    .map(BigPrimeExample::sort);

        Sink<List<BigInteger>, CompletionStage<Done>> printSink = Sink.foreach(System.out::println);
        
        CompletionStage<Done> result = intSource
            .via(bigIntFlow)
            // .buffer(16, OverflowStrategy.backpressure())
            .async()
            .via(bigPrimeFlowAsync)
            .async()
            .via(listFLow)
            .toMat(printSink, Keep.right())
            .run(actorSystem);

        // CompletionStage<Done> result = Source.range(1, 10)
        //         .map(i -> new BigInteger(3000, new Random()))
        //         .map(BigInteger::nextProbablePrime)
        //         .grouped(10)
        //         .map(BigPrimeExample::sort) 
        //         .toMat(printSink, Keep.right())
        //         .run(actorSystem);

        result.whenComplete((value, throwable) -> {
            long end = System.currentTimeMillis();
            actorSystem.log().info("Execution took {} seconds", (end - start) / 1000);
            actorSystem.terminate();
        });

    }

    private static List<BigInteger> sort(List<BigInteger> list) {
        List<BigInteger> sortedList = new ArrayList<>(list);
        sortedList.sort(Comparator.naturalOrder());
        return sortedList;
    }
}
