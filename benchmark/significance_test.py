# python3 significance_test.py
# -*- coding: utf-8 -*-
# ===========================================================================================
# @author 1Zero64
# Description: Displays plots for normal distribution of the materializer execution times
# ===========================================================================================

import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import norm
import pandas as pd


# Function to plot and display normal distribution with histogram for given materializer execution times
# @input df = process latencies as data frame
# @input name = name of the programming language
def plot_normal_distribution(df, name):
    # Get statistical values from data frame
    mu = df["process_latency"].mean()
    sigma = df["process_latency"].std()
    n_bins = df["process_latency"].count()

    # Create figure
    fig, axes = plt.subplots(nrows=1, ncols=1, sharex=True, figsize=(8, 5))

    # Plot histogram with
    n, bins, patches = axes.hist(df["process_latency"], n_bins, density=True, alpha=.1, edgecolor='black')

    # Calculate probability distribution function
    pdf = 1 / (sigma * np.sqrt(2 * np.pi)) * np.exp(-(bins - mu) ** 2 / (2 * sigma ** 2))
    # Get median, first and third quantile
    median, q1, q3 = np.percentile(df["process_latency"], 50), np.percentile(df["process_latency"], 25), np.percentile(df["process_latency"], 75)

    # Plot probability density function
    axes.plot(bins, pdf, color='orange', alpha=.6)

    # Fill in area from Q1-1.5*IQR to Q1 and Q3 to Q3+1.5*IQR
    iqr = 1.5 * (q3 - q1)
    x1 = np.linspace(q1 - iqr, q1)
    x2 = np.linspace(q3, q3 + iqr)
    pdf1 = 1 / (sigma * np.sqrt(2 * np.pi)) * np.exp(-(x1 - mu) ** 2 / (2 * sigma ** 2))
    pdf2 = 1 / (sigma * np.sqrt(2 * np.pi)) * np.exp(-(x2 - mu) ** 2 / (2 * sigma ** 2))
    axes.fill_between(x1, pdf1, 0, alpha=.6, color='orange')
    axes.fill_between(x2, pdf2, 0, alpha=.6, color='orange')

    # Add percentages as bottom text
    axes.annotate("{:.1f}%".format(100 * (norm(mu, sigma).cdf(q1) - norm(mu, sigma).cdf(q1 - iqr))), xy=(q1 - iqr / 2, 0), ha='center')
    axes.annotate("{:.1f}%".format(100 * (norm(mu, sigma).cdf(q3) - norm(mu, sigma).cdf(q1))), xy=(median, 0), ha='center')
    axes.annotate("{:.1f}%".format(100 * (norm(mu, sigma).cdf(q3 + iqr) - norm(mu, sigma).cdf(q3))), xy=(q3 + iqr / 2, 0), ha='center')

    # Add dashed lines for quantiles
    plt.axvline(df["process_latency"].quantile(0), color='b', linestyle='-.')
    plt.axvline(df["process_latency"].quantile(0.25), color='g', linestyle='--')
    plt.axvline(df["process_latency"].quantile(0.50), color='g', linestyle='--')
    plt.axvline(df["process_latency"].quantile(0.75), color='b', linestyle='--')
    plt.axvline(df["process_latency"].quantile(1), color='r', linestyle='-.')

    # Add labels and titles before showing the plot
    plt.title("Normalverteilung der Ausführungszeiten in " + name)
    axes.set_xlabel("Ausführungszeit in Sekunden", fontsize=14)
    axes.set_ylabel("Wahrscheinlichkeitsdichte", fontsize=14)
    plt.show()


if __name__ == '__main__':
    # Execution times for Go materializer from microbenchmark imported
    go = [19.584, 19.592, 19.719, 19.787, 19.798, 19.803, 19.908, 19.987, 20.007, 20.131, 20.149, 20.167, 20.213,
          20.241,
          20.318, 20.340, 20.363, 20.386, 20.453, 20.528, 20.571, 20.677, 20.747, 20.767,
          20.814, 20.873, 20.976, 20.978, 20.993, 21.053, 21.058, 21.147, 21.175, 21.632, 21.689, 21.799, 21.845,
          21.969,
          21.969, 22.036, 22.078, 22.321]

    # Execution times for Java materializer from microbenchmark imported
    java = [26.807, 27.774, 28.096, 28.266, 28.802, 28.972, 29.064, 29.224, 29.412, 29.447, 29.544, 29.57,
            29.733, 29.744, 29.937, 30.124, 30.132, 30.452, 30.508, 30.576, 30.589,
            31.613, 32.478, 33.533, 33.686, 33.704, 33.727, 33.946, 33.97, 34.267, 34.301, 34.379, 34.76,
            34.781, 34.861, 35.273, 35.312, 35.351, 35.693, 36.198, 37.283, 38.051]

    # Convert lists to data frames
    df_go = pd.DataFrame(go, columns=['process_latency'])
    df_java = pd.DataFrame(java, columns=['process_latency'])

    # Call plot function
    plot_normal_distribution(df_go, "Go")
    plot_normal_distribution(df_java, "Java")
