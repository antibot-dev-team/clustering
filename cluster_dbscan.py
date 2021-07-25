import argparse
import ast

from sklearn.cluster import DBSCAN

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def clustering(csv_file: str, int_len: int, min_req: int) -> None:
    """
    Cluster clients using RPI and deviation. Plot result.
    :param int_len: Length of interval in RPI
    :param min_req: Minimal amount of request done by client to be considered for clustering
    :param csv_file: CSV file containing RPI and Deviation columns
    :return: None
    """

    requests = pd.read_csv(
        csv_file,
        dtype={"Deviation": np.float64},
        converters={f"RPI{int_len}": ast.literal_eval},
    )

    # Remove rows with None values
    requests = requests.dropna()

    # Remove sessions with less than 5 requests
    requests = requests[requests["Diff"].apply(lambda x: len(x) + 1 >= min_req)]

    deviations = requests["Deviation"]
    rpis = [rpi[0] for rpi in requests[f"RPI{int_len}"]]
    data = np.array([[rpi, deviation] for rpi, deviation in zip(rpis, deviations)])

    # data = sklearn.preprocessing.StandardScaler().fit_transform(data)
    db = DBSCAN(eps=10, min_samples=4, metric="euclidean").fit(data)

    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Number of points: %d" % len(data))

    # Write client, data for clustering and its label to ./dumps/labeling.csv
    requests = requests[["IP", "UA", "Session"]]
    requests["Label"] = labels
    requests[f"RPI{int_len}"] = rpis
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

    plt.xlabel(f"RPI{int_len}")
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
    parser.add_argument(
        "--interval-length",
        dest="int_len",
        metavar="l",
        type=int,
        help="Length of interval for RPI column",
        default=30,
    )
    parser.add_argument(
        "--min-req",
        dest="min_req",
        metavar="m",
        type=int,
        help="Minimal amount of requests done by client in one session to be considered for clustering",
        default=5,
    )

    args = parser.parse_args()
    clustering(args.input, args.int_len, args.min_req)
