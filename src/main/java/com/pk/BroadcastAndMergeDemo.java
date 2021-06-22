package com.pk;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.concurrent.CompletionStage;

public class BroadcastAndMergeDemo {
    public static void main(String[] args) {
        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "actorsystem");
        Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(System.out::println);

        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(sink, (builder, out) -> {
                SourceShape<Integer> sourceShape = builder.add(Source.range(1, 10));
                FlowShape<Integer, Integer> addOneShape = builder.add(
                    Flow.of(Integer.class).map(i -> {
                        // System.out.println("Add one flow received: " + i);
                        return i + 1;
                    }
                ));
                FlowShape<Integer, Integer> doubleShape = builder.add(
                    Flow.of(Integer.class).map(i -> {
                        // System.out.println("Double flow received: " + i);
                        return i * 2;
                    }
                ));
                UniformFanOutShape<Integer, Integer> broadcastShape = builder.add(Broadcast.create(2));
                UniformFanInShape<Integer, Integer> mergeShape = builder.add(Merge.create(2));
                
                builder.from(sourceShape).toFanOut(broadcastShape);
                builder.from(broadcastShape.out(0)).via(addOneShape);
                builder.from(broadcastShape.out(1)).via(doubleShape);
                builder.from(addOneShape).toInlet(mergeShape.in(0));
                builder.from(doubleShape).toInlet(mergeShape.in(1));
                builder.from(mergeShape).to(out);

                return ClosedShape.getInstance();
            })
        );
        CompletionStage<Done> result = graph.run(actorSystem);
        result.whenComplete((value, throwable) -> actorSystem.terminate());
    }
}