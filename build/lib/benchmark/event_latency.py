from database.database import data_frame_from_materialized_view
import matplotlib.pyplot as plt


def displayEventLatencyKinesis(df):
    df = df[df.event_stream == "Kinesis"]

    plt.plot(df.processed_on, df.latency)
    plt.title("Event-Latenz Kinesis")
    plt.ylabel("Millisekunden")
    plt.xlabel("Zeitpunkt")
    plt.show()

    printStatistics(df)


def displayEventLatencyKafka(df):
    df = df[df.event_stream == "Kafka"]

    plt.plot(df.processed_on, df.latency)
    plt.title("Event-Latenz Kafka")
    plt.ylabel("Millisekunden")
    plt.xlabel("Zeitpunkt")
    plt.show()

    printStatistics(df)


def displayEventLatencyChangeStreams(df):
    df = df[df.event_stream == "Change Streams"]

    plt.plot(df.processed_on, df.latency)
    plt.title("Event-Latenzzeit Change Streams")
    plt.ylabel("Millisekunden")
    plt.xlabel("Zeitpunkt")
    plt.show()

    printStatistics(df)


def printStatistics(df):
        print("Anzahl der Datensätze:", df.latency.count())
        print("Standardabweichung:", df.latency.std())
        print("Median:", df.latency.median())
        print("Durchschnitt:", df.latency.mean())
        print("Niedrigster Wert:", df.latency.min())
        print("Höchster Wert:", df.latency.max())
        print("25% Perzentil:", df.latency.quantile(0.25))
        print("50% Perzentil:", df.latency.quantile(0.5))
        print("75% Perzentil:", df.latency.quantile(0.75))
        print("90% Perzentil:", df.latency.quantile(0.9))
        print("95% Perzentil:", df.latency.quantile(0.95))


if __name__ == '__main__':
    materialized_view = data_frame_from_materialized_view()

    displayEventLatencyChangeStreams(materialized_view)
