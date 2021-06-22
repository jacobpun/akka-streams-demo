package com.pk;

import java.util.concurrent.CompletionStage;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.time.Duration;

public class PartitionExample {
    public static void main(String[] args) {
        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");
        RunnableGraph<CompletionStage<Integer>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(
                Sink.fold(0, (Integer sum, Integer n) -> {
                    System.out.println(n);
                    return sum + n;
                }), 
                (builder, out) -> {
                    SourceShape<Integer> sourceNumbersShape = builder.add(Source.range(1, 10).throttle(1, Duration.ofSeconds(1)));
                    FlowShape<Integer, Integer> printFlowShape = builder.add(Flow.of(Integer.class).map(x -> {
                        System.out.println("Flow Received: " + x);
                        return x;
                    }).async());
                    UniformFanOutShape<Integer, Integer> partition = builder.add(Partition.create(2, x -> (x == 4 || x == 7)? 0: 1));
                    UniformFanInShape<Integer, Integer> merge = builder.add(Merge.create(2));
                    builder.from(sourceNumbersShape).toFanOut(partition);
                    builder.from(partition.out(0)).via(printFlowShape).viaFanIn(merge).to(out);
                    builder.from(partition.out(1)).viaFanIn(merge);        
                    return ClosedShape.getInstance();
                }
            )
        );
        CompletionStage<Integer> done = graph.run(actorSystem);
        done.whenComplete((result, throwable) -> {
            if (throwable == null) {
                System.out.println("materialized value: " + result);
            }
            actorSystem.terminate();
        });
    }
}
