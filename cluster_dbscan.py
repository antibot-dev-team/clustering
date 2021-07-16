import sklearn
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

import matplotlib.pyplot as plt
import numpy as np

import argparse
import json


def clustering(loginterval: str, logdeviation: str) -> None:
    """
    Cluster clients using RPI and deviation. Plot result.
    :param loginterval: JSON of the form {"IP:UA": [amount of requests per intervals]}
    :param logdeviation: JSON of the form {"IP:UA": int(standard deviation)}
    :return: None
    """

    with open(loginterval, "r") as interfile:
        data_interval = json.load(interfile)

    with open(logdeviation, "r") as interfile:
        data_deviation = json.load(interfile)

    data = np.array(
        [
            [intervals[0], deviation]
            for intervals, deviation in zip(
                data_interval.values(), data_deviation.values()
            )
        ]
    )

    # NOTE: value for alg with normalization: eps = 0.5
    data = sklearn.preprocessing.StandardScaler().fit_transform(data)

    # TODO: try reducing leaf_size for using less RAM
    db = DBSCAN(eps=0.5, min_samples=4, metric="euclidean").fit(data)

    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Estimated number of points: %d" % len(data))

    # pprint(dict(zip(data_interval.keys(), labels)))

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
        "--rpi",
        metavar="r",
        type=str,
        help="JSON with the amount of requests per given intervals",
        default="./dumps/log_clients_30s_100k.json",
    )
    parser.add_argument(
        "--deviation",
        metavar="d",
        type=str,
        help="JSON with the standard deviation for rpi",
        default="./dumps/log_clients_deviation_100k.json",
    )

    args = parser.parse_args()
    clustering(args.rpi, args.deviation)
