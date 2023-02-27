# python3 throughput.py
# -*- coding: utf-8 -*-
# ===========================================================================================
# @author 1Zero64
# Description: Displays plots for the throughput every second of the different streaming technologies
# ===========================================================================================

from database.database import data_frame_from_event_store
import matplotlib.pyplot as plt


# Function to plot and display throughput for a given, filtered stream
# @input df = materialized view data frame with the data
# @input event_stream = name of the stream to be filtered and displayed
def display_throughput(df, event_stream):
    # Filter data in data frame for the given stream
    df = df[df.event_stream == event_stream]

    # Check if data frame is empty
    if df.empty:
        print("No data found for materialized view: " + event_stream)
        return

    # Convert processed_on attribute to date time without milliseconds
    df["processed_on"] = df["processed_on"].values.astype("datetime64[s]")

    # Get first starting processed_on point of data
    start_point = df.processed_on.iloc[1]
    # Calculate elapsed time for every row with starting point
    df["time_elapsed"] = (df.processed_on - start_point).astype("timedelta64[s]")

    # Calculate throughput by grouping elapsed time together and counting the rows
    throughput = df.groupby("time_elapsed")["id"].count().rename("throughput")
    # Calculate average throughput
    average_throughput = round(throughput.values.mean(), 2)

    # Plot and display the throughput statistics
    plt.figure(figsize=(7, 4))
    plt.plot(throughput.keys(), throughput.values)
    # Title with average
    plt.title("Durchsatz " + event_stream + "\nDurchschnittlicher Durchsatz (rot): " + str(average_throughput))
    # Plot average as a red dashed line
    plt.axhline(y=average_throughput, color="r", linestyle="--")
    # Label the axes
    plt.ylabel("Verarbeitete Events pro Sekunde", fontsize=12)
    plt.xlabel("Vergangene Zeit in Sekunden", fontsize=11)
    # Display plot
    plt.show()


# Main for testing purposes
if __name__ == '__main__':
    event_store = data_frame_from_event_store()

    display_throughput(data_frame_from_event_store(), "Change Streams")
