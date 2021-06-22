package com.pk.vehiclespeed;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FlowShape;
import akka.stream.SourceShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

public class Main {
    public static void main(String[] args) {
        ActorSystem<Void> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");

        Map<Integer, VehiclePositionMessage> vehicleTrackingMap = new HashMap<>();
        for (int i = 1; i <=8; i++) {
            vehicleTrackingMap.put(i, new VehiclePositionMessage(1, new Date(), 0,0));
        }

        //source - repeat some value every 10 seconds.
        Source<Integer, NotUsed> everySecond = Source.repeat(1).throttle(1, Duration.ofSeconds(1));

        //flow 1 - transform into the ids of each van (ie 1..8) with mapConcat
        Flow<Integer, Integer, NotUsed> vehicleIds = Flow.of(Integer.class).mapConcat(i -> vehicleTrackingMap.keySet());

        //flow 2 - get position for each van as a VPMs with a call to the lookup method (create a new instance of
        //utility functions each time). Note that this process isn't instant so should be run in parallel.
        // Flow<Integer, VehiclePositionMessage, NotUsed> vehiclePositions = Flow.of(Integer.class).mapAsyncUnordered(vehicleTrackingMap.size(), id -> {
        //     CompletableFuture<VehiclePositionMessage> futurePosition = new CompletableFuture<>();
        //     futurePosition.completeAsync(() -> new UtilityFunctions().getVehiclePosition(id));
        //     return futurePosition;
        // });
        Flow<Integer, VehiclePositionMessage, NotUsed> vehiclePositions = Flow.of(Integer.class).map(new UtilityFunctions()::getVehiclePosition);

        //flow 3 - use previous position from the map to calculate the current speed of each vehicle. Replace the
        // position in the map with the newest position and pass the current speed downstream
        Flow<VehiclePositionMessage, VehicleSpeed, NotUsed> vehicleSpeeds = Flow.of(VehiclePositionMessage.class).map(currentPosition -> {
            VehiclePositionMessage previousPosition = vehicleTrackingMap.get(currentPosition.getVehicleId());
            VehicleSpeed speed = new UtilityFunctions().calculateSpeed(previousPosition, currentPosition);
            vehicleTrackingMap.put(currentPosition.getVehicleId(), currentPosition);
            return speed;
        });

        //flow 4 - filter to only keep those values with a speed > 95
        Flow<VehicleSpeed, VehicleSpeed, NotUsed> filterForSpeedGT95 = Flow.of(VehicleSpeed.class).filter(speed -> speed.getSpeed() > 95);
        
        //sink - as soon as 1 value is received return it as a materialized value, and terminate the stream
        // Sink<VehicleSpeed, CompletionStage<VehicleSpeed>> sink = Sink.fold(null, (acc, curr) -> curr);
        Sink<VehicleSpeed, CompletionStage<VehicleSpeed>> sink = Sink.head();
        
        // CompletionStage<VehicleSpeed> resultFuture = everySecond
        //     .via(vehicleIds)
        //     .async()
        //     .via(vehiclePositions)
        //     .async()
        //     .via(vehicleSpeeds)
        //     .via(filterForSpeedGT95)
        //     .toMat(sink, Keep.right())
        //     .run(actorSystem);
        
        RunnableGraph<CompletionStage<VehicleSpeed>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(sink, (builder, out) -> {
                SourceShape<Integer> sourceShape = builder.add(everySecond);
                FlowShape<Integer, Integer> vehicleIdsShape = builder.add(vehicleIds);
                // FlowShape<Integer, VehiclePositionMessage> vehiclePositionsShape = builder.add(vehiclePositions.async());
                FlowShape<VehiclePositionMessage, VehicleSpeed> vehicleSpeedShape = builder.add(vehicleSpeeds);
                FlowShape<VehicleSpeed, VehicleSpeed> filterForSpeedGT95Shape = builder.add(filterForSpeedGT95);
                UniformFanOutShape<Integer, Integer> balance = builder.add(Balance.create(8, true));
                UniformFanInShape<VehiclePositionMessage, VehiclePositionMessage> merge = builder.add(Merge.create(8));

                builder.from(sourceShape)
                        .via(vehicleIdsShape)
                        .viaFanOut(balance);

                for (int i=0; i< 8; i++) {
                    builder.from(balance).via(builder.add(vehiclePositions.async())).viaFanIn(merge);
                }

                builder.from(merge).via(vehicleSpeedShape).via(filterForSpeedGT95Shape).to(out);

                return ClosedShape.getInstance();
            })
        );

        long start = System.currentTimeMillis();
        CompletionStage<VehicleSpeed> resultFuture = graph.run(actorSystem);

        resultFuture.whenComplete((result, throwable) -> {
            if (throwable == null) {
                actorSystem.log().info("Winner: Vehicle: {}. Speed: {}", result.getVehicleId(), result.getSpeed());
            } else {
                actorSystem.log().error("Something went wrong. {}", throwable);;
            }
            long end = System.currentTimeMillis();
            actorSystem.log().info("Total time: " + (end - start) + ".ms");
            actorSystem.terminate();
        });
    }
}