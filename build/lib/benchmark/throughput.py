from database.database import data_frame_from_event_store
import matplotlib.pyplot as plt


def displayThroughputKinesis(df):
    df = df[df.event_stream == "Kinesis"]
    throughput = df.groupby("processed_on")["id"].count().rename("throughput")

    plt.plot(throughput.keys(), throughput.values)
    plt.title("Durchsatz Kinesis")
    plt.ylabel("Verarbeitete Events pro Sekunde")
    plt.xlabel("Zeitpunkt")
    plt.show()


def displayThroughputKafka(df):
    df = df[df.event_stream == "Kafka"]
    throughput = df.groupby("processed_on")["id"].count().rename("throughput")

    plt.plot(throughput.keys(), throughput.values)
    plt.title("Durchsatz Kafka")
    plt.ylabel("Verarbeitete Events pro Sekunde")
    plt.xlabel("Zeitpunkt")
    plt.show()


def displayThroughputChangeStreams(df):
    df = df[df.event_stream == "Change Streams"]
    throughput = df.groupby("processed_on")["id"].count().rename("throughput")

    plt.plot(throughput.keys(), throughput.values)
    plt.title("Durchsatz Change Streams")
    plt.ylabel("Verarbeitete Events pro Sekunde")
    plt.xlabel("Zeitpunkt")
    plt.show()


if __name__ == '__main__':
    event_store = data_frame_from_event_store()
    event_store["processed_on"] = event_store["processed_on"].astype("datetime64[s]")

    #displayThroughputKinesis(event_store)

    displayThroughputChangeStreams(event_store)

    #displayThroughputChangeStreams(event_store)
