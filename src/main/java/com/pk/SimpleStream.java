package com.pk;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class SimpleStream {
    public static void main(String[] args) {
        // Source<Integer, NotUsed> souce = Source.range(1, 10);
        // Source<Integer, NotUsed> source = Source.single(1);
        // Source<Integer, NotUsed> source = Source.from(List.of(1, 2, 5, 7, 11));
        // Source<Integer, NotUsed> source = Source.repeat(99);
        // Source<Integer, NotUsed> source = Source.cycle(List.of(1, 2, 3)::iterator);
        Source<Integer, NotUsed> source = Source
                                            .fromIterator(Stream.iterate(0, i -> i +1)::iterator)
                                            .throttle(1, Duration.ofSeconds(1))
                                            .take(5);
        Flow<Integer, String, NotUsed> flow = Flow.of(Integer.class).map(val -> "REVEIVED " + val);
        
        // Sink<String, CompletionStage<Done>> sink = Sink.ignore();
        Sink<String, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        RunnableGraph<NotUsed> graph = source.via(flow).to(sink);

        ActorSystem actorSystem = ActorSystem.create(Behaviors.empty(), "actorSystem");
        graph.run(actorSystem);
    }
}