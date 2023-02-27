# python3 event_latency.py
# -*- coding: utf-8 -*-
# ===========================================================================================
# @author 1Zero64
# Description: Displays plots for the event latency of processed events of the different streaming technologies
# ===========================================================================================

from database.database import data_frame_from_materialized_view
import matplotlib.pyplot as plt


# Function to plot and display event latency for a given, filtered stream
# @input df = materialized view data frame with the data
# @input event_stream = name of the stream to be filtered and displayed
def display_event_latency(df, event_stream):
    # Filter data in data frame for the given stream
    df = df[df.event_stream == event_stream]

    # Check if data frame is empty
    if df.empty:
        print("No data found for materialized view: " + event_stream)
        return

    # Get first starting processed_on point of data
    start_point = df.processed_on.iloc[1]
    # Calculate elapsed time for every row with starting point
    df["time_elapsed"] = (df.processed_on - start_point).astype("timedelta64[s]")

    # Plot and display the event latency
    plt.figure(figsize=(7, 4))
    plt.plot(df.time_elapsed, df.latency)
    plt.title("Event-Latenz " + event_stream)
    plt.ylabel("Millisekunden", fontsize=14)
    plt.xlabel("Vergangene Zeit in Sekunden", fontsize=11)
    plt.show()

    # Print event latency statistics
    print_statistics(df)


# Function to print event latency statistics like average, median, percentiles, min and max
# @input df = materialized view data frame with the data
def print_statistics(df):
    print()
    print("================================================================")
    print("{:25} {:d}".format("Anzahl der Datensaetze:", df.latency.count()))
    print("{:25} {:.2f}ms".format("Standardabweichung:", df.latency.std()))
    print("{:25} {:.2f}ms".format("Median:", df.latency.median()))
    print("{:25} {:.2f}ms".format("Durchschnitt:", df.latency.mean()))
    print("{:25} {:.2f}ms".format("Niedrigster Wert:", df.latency.min()))
    print("{:25} {:.2f}ms".format("Höchster Wert:", df.latency.max()))
    print("{:25} {:.2f}ms".format("25% Perzentil:", df.latency.quantile(0.25)))
    print("{:25} {:.2f}ms".format("50% Perzentil:", df.latency.quantile(0.5)))
    print("{:25} {:.2f}ms".format("75% Perzentil:", df.latency.quantile(0.75)))
    print("{:25} {:.2f}ms".format("90% Perzentil:", df.latency.quantile(0.9)))
    print("{:25} {:.2f}ms".format("95% Perzentil:", df.latency.quantile(0.95)))
    print("{:25} {:.2f}ms".format("99% Perzentil:", df.latency.quantile(0.99)))
    print("================================================================")
    print("")


# Main for testing purposes
if __name__ == '__main__':
    display_event_latency(data_frame_from_materialized_view(), "Kafka")
