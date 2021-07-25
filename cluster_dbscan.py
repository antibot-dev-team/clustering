import argparse
import ast

import sklearn
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def clustering(csv_file: str) -> None:
    """
    Cluster clients using RPI and deviation. Plot result.
    :param csv_file: CSV file containing RPI and Deviation columns
    :return: None
    """

    # TODO: don't hardcode interval length
    requests = pd.read_csv(
        csv_file,
        dtype={"Deviation": np.float64},
        converters={"RPI30": ast.literal_eval},
    )

    requests = requests.dropna()

    deviations = requests["Deviation"]
    rpis = [rpi[0] for rpi in requests["RPI30"]]

    data = np.array([[rpi, deviation] for rpi, deviation in zip(rpis, deviations)])

    # NOTE: value for alg with normalization: eps = 0.5
    # data = sklearn.preprocessing.StandardScaler().fit_transform(data)

    db = DBSCAN(eps=10, min_samples=4, metric="euclidean").fit(data)

    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Estimated number of points: %d" % len(data))

    requests = requests[["IP", "UA", "Session"]]
    requests["Label"] = labels
    requests["RPI30"] = rpis
    requests["Deviation"] = deviations
    requests.to_csv("./dumps/labeling.csv", index=False)

    # Draw
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    core_samples_mask[db.core_sample_indices_] = True

    unique_labels = set(labels)
    colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]
    for k, col in zip(unique_labels, colors):
        if k == -1:
            # Black used for noise.
            col = [0, 0, 0, 1]

        class_member_mask = labels == k

        xy = data[class_member_mask & core_samples_mask]
        plt.plot(
            xy[:, 0],
            xy[:, 1],
            "o",
            markerfacecolor=tuple(col),
            markeredgecolor="k",
            markersize=4,
        )

        xy = data[class_member_mask & ~core_samples_mask]
        plt.plot(
            xy[:, 0],
            xy[:, 1],
            "o",
            markerfacecolor=tuple(col),
            markeredgecolor="k",
            markersize=4,
        )

    plt.xlabel("RPI")
    plt.ylabel("Mean deviation")
    plt.title("Estimated number of clusters: %d" % n_clusters_)
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cluster using DBSCAN algorithm.")
    parser.add_argument(
        "--input",
        metavar="i",
        type=str,
        help="CSV file with RPI and Deviation columns",
        default="./dumps/requests.csv",
    )

    args = parser.parse_args()
    clustering(args.input)
