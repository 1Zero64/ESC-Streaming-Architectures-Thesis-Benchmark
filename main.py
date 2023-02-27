# python3 main.py
# -*- coding: utf-8 -*-
# ===========================================================================================
# @author 1Zero64
# Description: Main program to control and call plot and display functions for the benchmark
# ===========================================================================================

from benchmark.throughput import display_throughput
from benchmark.event_latency import display_event_latency
from database.database import data_frame_from_event_store, data_frame_from_materialized_view

# Entry function, when application is executed
if __name__ == '__main__':

    # Load materialized view data into a data frame
    materialized_view_df = data_frame_from_materialized_view()
    print("Data Loaded")

    # Run application in a while loop until interruption or exit
    # Display function and wait on user input
    while True:
        print("0: Exit")
        print("1: Kinesis Event-Latenz")
        print("2: Kafka Event-Latenz")
        print("3: Change Streams Event-Latenz")
        print("4: Kinesis Durchsatz")
        print("5: Kafka Durchsatz")
        print("6: Change Streams Durchsatz")
        print("7: Reload Materialized View")
        method = input(": ")
        print("\n\n")

        # Execute corresponding function for user input
        if method == "0":
            exit()
        elif method == "1":
            display_event_latency(materialized_view_df, "Kinesis")
        elif method == "2":
            display_event_latency(materialized_view_df, "Kafka")
        elif method == "3":
            display_event_latency(materialized_view_df, "Change Streams")
        elif method == "4":
            display_throughput(materialized_view_df, "Kinesis")
        elif method == "5":
            display_throughput(materialized_view_df, "Kafka")
        elif method == "6":
            display_throughput(materialized_view_df, "Change Streams")
        elif method == "7":
            materialized_view_df = data_frame_from_materialized_view()
            print("Reloaded Materialized View Data")
        else:
            continue
