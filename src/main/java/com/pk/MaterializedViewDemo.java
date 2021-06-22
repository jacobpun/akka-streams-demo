package com.pk;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;

public class MaterializedViewDemo {
    public static void main(String[] args) {
        Random rand = new Random();
        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");
        Source<Integer, NotUsed> src = Source.repeat(1).map(i -> rand.nextInt(300));
        Flow<Integer, Integer, NotUsed> filterGreaterThan200 = Flow.of(Integer.class).filter(i -> i > 200);
        Flow<Integer, Integer, NotUsed> filterEven = Flow.of(Integer.class).filter(i -> i %2 == 0);
        Sink<Integer, CompletionStage<Done>> printSink = Sink.foreach(System.out::println);
        // Sink<Integer, CompletionStage<Integer>> printSinkWithCounter = Sink.fold(0, (counter, value) -> {
        //     System.out.println(value);
        //     return counter + 1;
        // });

        Sink<Integer, CompletionStage<Integer>> sumSink = Sink.reduce((acc, value) -> acc + value);

        CompletionStage<Integer> restult = src
                                            .throttle(1, Duration.ofSeconds(1))
                                            .log("log throttled value")
                                            .takeWithin(Duration.ofSeconds(5))
                                            .via(filterEven)
                                            .via(filterGreaterThan200.takeWhile(i -> i < 275))
                                            .toMat(sumSink, Keep.right())
                                            .run(actorSystem);
        restult.whenComplete((result, throwable) -> {
            if (throwable == null) {
                System.out.println("Graph's materiazlized value is: " + result);
            } else {
                System.out.println("Something went wrong: " + throwable);
            }
            actorSystem.terminate();
        });

        // src.toMat(printSink, Keep.right()).run(actorSystem);
        
    }
}
