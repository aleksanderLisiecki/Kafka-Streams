package com.example.bigdata;

import com.example.bigdata.serdesy.*;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.function.DoubleBinaryOperator;


public final class NetflixPrizeProcessing {

    public static final String INPUT_TOPIC = "kafka-producer-topic";
    public static final String OUTPUT_TOPIC = "kafka-consumer-topic";
    public static final String ANOMALIES_TOPIC = "kafka-anomalies-topic";

    private static Long D = 30L; // number of days
    private static Long L = 100L; // minimal number of counts
    private static Double O = 4.0; // minimal rates avg

    private static HashMap<Long, MovieTitleRecord> movieTitleList = new HashMap<>();

    //============================================
    // Config
    //============================================

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-netflix-prize-processor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, EventTimeExtractor.class);
        return config;
    }

    //============================================
    // Mięsko - streamsy
    //============================================

    static void createNetflixStream(final StreamsBuilder builder) {
        final Serde<NetflixPrizeRecord> netflixPrizeRecordSerde =
                Serdes.serdeFrom(new NetflixPrizeRecordSerializer(), new NetflixPrizeRecordDeserializer());
        final Serde<ETLAggregation> etlAggregationSerde =
                Serdes.serdeFrom(new ETLAggregationSerializer(), new ETLAggregationDeserializer());
        final Serde<AnomalyAggregation> anomalyAggregationSerde =
                Serdes.serdeFrom(new AnomalyAggregationSerializer(), new AnomalyAggregationDeserializer());


        final KStream<String, String> textLines = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, NetflixPrizeRecord> netflixStream = textLines
                .mapValues(NetflixPrizeRecord::parseFromCSVLine);

        final KStream<AggregationKey, NetflixPrizeRecord> keyNetflixStream = netflixStream
                .map((key, value) -> KeyValue.pair(new AggregationKey(
                        value.getYearAndMonth(),
                        value.getFilm_id(),
                        movieTitleList.get(value.getFilm_id())), value));

        //============================================
        // ETL
        //============================================

        KTable<Windowed<String>, ETLAggregation> netflixETL = keyNetflixStream
                .map((key, value) -> KeyValue.pair(key.toString(), value))
                .groupByKey(Grouped.with(Serdes.String(), netflixPrizeRecordSerde))

                // Nie można zrobić agregacji miesięcznej wg. kalendarza, poniżej link o tym mówiący
                // https://issues.apache.org/jira/browse/KAFKA-10408\
                // Zrobiłem agregację 30 dniową

                .windowedBy(TimeWindows.of(Duration.ofDays(30)).advanceBy(Duration.ofDays(1)))
                .aggregate(
                        ETLAggregation::new,
                        (aggKey, value, aggregate) -> aggregate.update(value),
                        Materialized.<String, ETLAggregation, WindowStore<Bytes, byte[]>>as("etl-store")
                                .withKeySerde(Serdes.String()).withValueSerde(etlAggregationSerde)
                );
//                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        netflixETL.toStream()
                .map((k,v)->KeyValue.pair(k.key(), k.key() + " " + v.toString()))
                .to(OUTPUT_TOPIC);

        //============================================
        // Anomalies
        //============================================

        KTable<Windowed<String>, AnomalyAggregation> netflixAnomalies = keyNetflixStream
                .groupBy((k,v) -> k.getKey_film_title(),Grouped.with(Serdes.String(), netflixPrizeRecordSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(D)).advanceBy(Duration.ofDays(1)))
                .aggregate(
                        AnomalyAggregation::new,
                        (aggKey, value, aggregate) -> aggregate.update(value),
                        Materialized.<String, AnomalyAggregation, WindowStore<Bytes, byte[]>>as("anomalies-store")
                                .withKeySerde(Serdes.String()).withValueSerde(anomalyAggregationSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

        netflixAnomalies.toStream().filter((k,v)-> (v.getNumberOfRates() >= L && v.getAverageDegree() >= O))
                .map((k, v)-> KeyValue.pair(k.key(),
                        "Start: " + LocalDateTime.ofInstant(k.window().startTime(), TimeZone.getDefault().toZoneId()).toString().substring(0,10) +
                                " stop: " + LocalDateTime.ofInstant(k.window().endTime(), TimeZone.getDefault().toZoneId()).toString().substring(0,10) +
                                " | title: " + k.key() +
                                " | # rates: " + v.getNumberOfRates() +
                                " | rates sum: " + v.getRatesSum() +
                                " | average degree: " + v.getAverageDegree()))
                .to(ANOMALIES_TOPIC);
    }

    //============================================
    // Main
    //============================================

    public static void main(final String[] args) throws IOException {
        switch (args.length) {
            case 5:
                O = Double.valueOf(args[4]);
            case 4:
                L = Long.valueOf(args[3]);
            case 3:
                D = Long.valueOf(args[2]);
        }

        System.out.println("D: " + D + " L: " + L + " O: " + O);

        movieTitleList = readMovieTitlesCSV(args[1]);

        Properties config = getStreamsConfig(args);

        final StreamsBuilder builder = new StreamsBuilder();
        createNetflixStream(builder);

//        final Topology topology = builder.build();
//        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-netflix-prize-processor-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    //============================================
    // Private methods
    //============================================

    private static HashMap<Long, MovieTitleRecord> readMovieTitlesCSV(String filepath) throws IOException {
        HashMap<Long, MovieTitleRecord> movieList = new HashMap();
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if(values[0].equals("ID"))
                    continue;
                if(values[1].equals("NULL"))
                    values[1] = "0000";
                movieList.put(Long.parseLong(values[0]), new MovieTitleRecord(Long.parseLong(values[1]), values[2]));
            }
        }
        return movieList;
    }

}