package com.pk.transactionmanager;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.ClosedShape;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.SinkShape;
import akka.stream.SourceShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.ZipWith;
import akka.stream.javadsl.Sink;

import java.math.BigDecimal;

import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class Main2PartialGraphs {
    public static void main(String[] args) {

        Map<Integer, Account> accounts = new HashMap<>();

        //set up accounts
        for (int i  = 1; i <= 10; i++) {
            accounts.put(i, new Account(i, new BigDecimal(1000)));
        }

        //source to generate 1 transaction every second
        Source<Integer, NotUsed> source = Source.repeat(1).throttle(1, Duration.ofSeconds(2));

        //flow to create a random transfer
        Flow<Integer, Transfer, NotUsed> generateTransfer = Flow.of(Integer.class).map (x -> {
            Random r = new Random();
            int accountFrom = r.nextInt(9) + 1;
            int accountTo;
            do {
                 accountTo = r.nextInt(9) + 1;
            } while (accountTo == accountFrom);

            BigDecimal amount = new BigDecimal(r.nextInt(100000)).divide(new BigDecimal(100));
            Date date = new Date();

            Transaction from = new Transaction(accountFrom, BigDecimal.ZERO.subtract(amount), date);
            Transaction to = new Transaction(accountTo, amount, date);
            return new Transfer(from,to);
        });

        Sink<Transfer, CompletionStage<Done>> logSink = Sink.foreach(t -> System.out.println(
            "Transfer $" + t.getTo().getAmount() +
            " from account " + t.getFrom().getAccountNumber() +
            " to account " + t.getTo().getAccountNumber() + "."));
        
        Sink<Transaction, CompletionStage<Done>> logNegativeTransactionSink = Sink.foreach(t -> System.out.println(
            "Transaction " + t + " is rejected as it results in negative account balance. " +
            "Current balance: " + accounts.get(t.getAccountNumber()).getBalance()
        ));    

        Flow<Transaction, Transaction, NotUsed> applyTransactionToAccountFlow = Flow.of(Transaction.class)
            .map(t -> {
                Account account = accounts.get(t.getAccountNumber());
                account.addTransaction(t);
                return t;
            });

        Source<Integer, NotUsed> transactionIds = Source.fromIterator(
            () -> Stream.iterate(1, i -> i+1).iterator()
        );

        Flow<Transfer, Transaction, NotUsed> splitToTransactions = Flow.of(Transfer.class)
            .mapConcat(transfer -> List.of(
                transfer.getFrom(), 
                transfer.getTo()
            ));
       

        Graph<SourceShape<Transaction>, NotUsed> sourcePartialGraph = GraphDSL.create(builder -> {
            FanInShape2<Integer, Transaction, Transaction> assignTransactionId = builder.add(
                ZipWith.create( (id, trans) -> {
                    trans.setUniqueId(id);
                    return trans;
                })
            );
            builder.from(builder.add(source))
                .via(builder.add(generateTransfer.alsoTo(logSink)))
                .via(builder.add(splitToTransactions))
                .toInlet(assignTransactionId.in1());

            builder.from(builder.add(transactionIds)).toInlet(assignTransactionId.in0());
            return SourceShape.of(assignTransactionId.out());
        });

        Graph<SinkShape<Transaction>, CompletionStage<Done>> sinkPartialGraph = GraphDSL.create(
            Sink.foreach(System.out::println),
            (builder, out) -> {
                FlowShape<Transaction, Transaction> entryFlow = builder.add(
                    Flow.of(Transaction.class)
                        .divertTo(
                            logNegativeTransactionSink, 
                            t -> {
                                BigDecimal forecastBalance = accounts.get(t.getAccountNumber()).getBalance().add(t.getAmount());
                                return forecastBalance.compareTo(BigDecimal.ZERO) < 0;
                            }
                        )
                );

                builder.from(entryFlow)
                        .via(builder.add(applyTransactionToAccountFlow))
                        .to(out);
                return SinkShape.of(entryFlow.in());
            }
        );

        RunnableGraph<CompletionStage<Done>> graph = RunnableGraph.fromGraph(
            GraphDSL.create(
                sinkPartialGraph,
                (builder, out) -> {
                    builder.from(builder.add(sourcePartialGraph)).to(out);
                    return ClosedShape.getInstance();
                }
            )
        );

        ActorSystem<?> actorSystem = ActorSystem.create(Behaviors.empty(), "actor-system");
        graph.run(actorSystem);
    }
}
