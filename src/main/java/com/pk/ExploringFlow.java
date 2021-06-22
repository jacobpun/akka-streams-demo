package com.pk;

import java.util.List;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;

public class ExploringFlow {
    public static void main(String[] args) {
        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "ActorSystem");

        Source<Integer, NotUsed> numbersSource = Source.range(1, 200);
        Flow<Integer, Integer, NotUsed> divisibleBy17 = Flow.of(Integer.class).filter(i -> i%17 == 0);
        Flow<Integer, Integer, NotUsed> mapConcatFlow = Flow.of(Integer.class).mapConcat(i -> List.of(i, i+1, i+2));
        Flow<Integer, Integer, NotUsed> groupedFlow = Flow.of(Integer.class)
                    .grouped(3)
                    .map(val -> List.of(val.get(2), val.get(1), val.get(0)))
                    .mapConcat(val -> val);
        Sink<Integer, CompletionStage<Done>> printSink = Sink.foreach(System.out::println);
        // Sink<List<Integer>, CompletionStage<Done>> printListSink = Sink.foreach(System.out::println);


        numbersSource
            .via(divisibleBy17)
            .via(mapConcatFlow)
            .via(groupedFlow)
            .to(printSink)
            //.to(printListSink)
            .run(actorSystem);

    }
}
