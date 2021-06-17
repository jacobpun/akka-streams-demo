package com.pk;

import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;

public class ConcurrencyDemoUsingBalancing {
    public static void main(String[] args) {
        Source<Integer, NotUsed> source = Source.range(1, 10);
        Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class).map(v -> {
            System.out.println("Received " + v);
            try {
                Thread.sleep(3000);
            } catch (Exception e) {}
            
            System.out.println("Returning " + v);
            return v;
        });
        Sink<Integer, CompletionStage<Done>> sink = Sink.foreach(System.out::println);
        
        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(sink, (builder, out) -> {
                SourceShape<Integer> sourceShape = builder.add(source);
                
                UniformFanOutShape<Integer, Integer> balance = builder.add(
                        Balance.create(4, true) // IMP: the second param 'true' is needed
                    );
                UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(4));
                builder.from(sourceShape).viaFanOut(balance);
                for (int i =0; i<4; i++) {
                    builder.from(balance).via(builder.add(flow.async())).viaFanIn(merge);
                }
                builder.from(merge).to(out);
                return ClosedShape.getInstance();
            })
        );

        ActorSystem<Void> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");

        long start = System.currentTimeMillis();
        CompletionStage<Done> future = graph.run(actorSystem);
        future.whenComplete((result, throwable) -> {
            long end = System.currentTimeMillis();
            System.out.println("Time taken - " + (end - start) + ".ms");
            actorSystem.terminate();
        });
    }
}
