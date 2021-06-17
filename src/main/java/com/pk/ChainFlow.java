package com.pk;

import java.util.List;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class ChainFlow {
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "ActorSystem");
        
        Source<String, NotUsed> sentances = Source.from(List.of(
            "Birds fly high in sky",
            "Ocean is Blue",
            "Jack and Jill went up the hill"
        ));
        Flow<String, Integer, NotUsed> wordsCountFlow = Flow.of(String.class).map(str -> str.split("\\b")).map(list -> list.length);

        Source<Integer, NotUsed> wordsCountSource = sentances.via(wordsCountFlow);

        Source<Integer, NotUsed> wordsCountSource2 = Source
            .from(
                List.of(
                    "Birds fly high in sky",
                    "Ocean is Blue",
                    "Jack and Jill went up the hill"
                )
            )
            .map(str -> str.split("\\b"))
            .map(list -> list.length);

        Sink<Integer, CompletionStage<Done>> printSink = Sink.foreach(System.out::println);
    }
}
