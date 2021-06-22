package com.pk;

import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.japi.tuple.Tuple3;
import akka.stream.ClosedShape;
import akka.stream.FanInShape3;
import akka.stream.FanOutShape3;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.UnzipWith;
import akka.stream.javadsl.ZipWith;
import akka.stream.javadsl.GraphDSL.Builder;

public class NonUniformFanoutAndFanInExample {
    public static void main(String[] args) {
        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(
                Sink.foreach(System.out::println),
                (build, out) -> {
                    SourceShape<Integer> source = build.add(Source.range(1, 10));

                    FlowShape<String, String> strFlow = getFlowShape(build, String.class);
                    FlowShape<Integer, Integer> intFlow = getFlowShape(build, Integer.class);
                    FlowShape<Boolean, Boolean> boolFlow = getFlowShape(build, Boolean.class);

                    FanOutShape3<Integer, String, Boolean, Integer> fanOut = build.add(
                        UnzipWith.create3(i -> new Tuple3<>("Number-" + i, i %2 == 0, i))
                    );

                    FanInShape3<String, Boolean, Integer, String> fanIn = build.add(
                        ZipWith.create3((s, b, i) ->  "Received " + i + " (" + (b? "even": "odd") + ") str: " + s + ".")
                    );

                    build.from(source).toInlet(fanOut.in());
                    build.from(fanOut.out0()).via(strFlow).toInlet(fanIn.in0());
                    build.from(fanOut.out1()).via(boolFlow).toInlet(fanIn.in1());
                    build.from(fanOut.out2()).via(intFlow).toInlet(fanIn.in2());
                    build.from(fanIn.out()).to(out);

                    return ClosedShape.getInstance();
                }
            )
        );

        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");
        CompletionStage<Done> completionStage = graph.run(actorSystem);
        completionStage.whenComplete((done, throwable) -> actorSystem.terminate());
    }

    private static <T> FlowShape<T, T> getFlowShape(Builder<CompletionStage<Done>> build, Class<T> clazz) {
        return build.add(Flow.of(clazz).map(v -> {
            System.out.println(clazz.getSimpleName() + " flow: " + v);
            return v;
        }));
    }
}
