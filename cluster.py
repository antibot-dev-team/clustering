import argparse
import ast

import sklearn
from numpy import array
from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def two_dim_clustering(
    csv_file: str,
    algorithm: str,
    usg_data: str,
    int_len: int,
    min_req: int,
    eps: int,
    min_samples: int,
    n_clusters: int,
) -> None:

    """
    Two-dimensional cluster clients using RPI and deviation or mean. Plot result.

    :param csv_file: CSV file containing RPI and Deviation columns
    :param algorithm: The algorithm for clustering. Available: dbscan, kmeans
    :param usg_data: Data for clustering. Available: Deviation, Mean
    :param int_len: Length of interval in RPI
    :param min_req: Minimal amount of request done by client to be considered for clustering
    :param eps: Epsilon parameter for DBSCAN
    :param min_samples: Minimal samples parameter for DBSCAN
    :param n_clusters: Number of clusters for kmeans
    :return: None
    """

    requests = pd.read_csv(
        csv_file,
        dtype={f"{usg_data}": np.float64},
        converters={f"RPI{int_len}": ast.literal_eval},
    )

    # Remove rows with None values
    requests = requests.dropna(axis=0, how="any")

    # Remove sessions with less than 5 requests
    requests = requests[requests["Diff"].apply(lambda x: len(x) + 1 >= min_req)]

    second_param = requests[f"{usg_data}"]

    rpis = [rpi[0] for rpi in requests[f"RPI{int_len}"]]
    data = np.array([[rpi, deviation] for rpi, deviation in zip(rpis, second_param)])

    # TODO normalization
    # data = sklearn.preprocessing.StandardScaler().fit_transform(data)
    db = (
        KMeans(n_clusters=n_clusters, random_state=0).fit(data)
        if algorithm == "kmeans"
        else DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit(data)
    )
    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print(f"=== 2d clustering for RPI{int_len} and {usg_data} ===")
    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Number of points: %d\n" % len(data))

    # Write client, data for clustering and its label to ./dumps/labeling.csv
    requests = requests[["IP", "UA", "Session"]]
    requests["Label"] = labels
    requests[f"RPI{int_len}"] = rpis
    requests[f"{usg_data}"] = second_param
    requests.to_csv(f"./dumps/2d_{algorithm}_labeling.csv", index=False)

    # Dump top-100 interesting clients
    sort = requests.sort_values(by=f"RPI{int_len}", ascending=False)
    sort[:100].to_csv(f"./dumps/top_most_RPI{int_len}.csv", index=False)

    sort = sort.sort_values(by=f"{usg_data}", ascending=True)
    sort[:100].to_csv(f"./dumps/top_least_{usg_data}.csv", index=False)

    # Draw
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
    # core_samples_mask[db.core_sample_indices_] = True

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
            markersize=8,
            alpha=0.2,
        )

        xy = data[class_member_mask & ~core_samples_mask]
        plt.plot(
            xy[:, 0],
            xy[:, 1],
            "o",
            markerfacecolor=tuple(col),
            markeredgecolor="k",
            markersize=6,
            alpha=0.2,
        )

    plt.xlabel(f"RPI{int_len}")
    plt.ylabel(f"{usg_data}")
    plt.title("Estimated number of clusters: %d" % n_clusters_)
    plt.show()


def one_dim_clustering(
    csv_file: str,
    algorithm: str,
    usg_data: str,
    int_len: int,
    min_req: int,
    eps: int,
    min_samples: int,
    n_clusters: int,
) -> None:

    """
    One-dimensional cluster clients using RPI or deviation or mean. Plot result.

    :param csv_file: CSV file containing RPI and Deviation columns
    :param algorithm: The algorithm for clustering. Available: dbscan, kmeans
    :param usg_data: Data for clustering. Available: Deviation, Mean
    :param int_len: Length of interval in RPI
    :param min_req: Minimal amount of request done by client to be considered for clustering
    :param eps: Epsilon parameter for DBSCAN
    :param min_samples: Minimal samples parameter for DBSCAN
    :param n_clusters: Number of clusters for kmeans
    :return: None
    """

    # Converters for different types of data: RPI is str->list, Mean and Dev is a float64
    converters = (
        {f"{usg_data}": ast.literal_eval} if f"{usg_data}" == f"RPI{int_len}" else None
    )
    requests = pd.read_csv(csv_file, converters=converters)
    requests = requests.dropna()
    requests = requests[requests["Diff"].apply(lambda x: len(x) + 1 >= min_req)]

    datas = array(
        [
            data[0] if f"{usg_data}" == f"RPI{int_len}" else data
            for data in requests[f"{usg_data}"]
        ]
    ).reshape(-1, 1)

    db = (
        KMeans(n_clusters=n_clusters, random_state=0).fit(datas)
        if algorithm == "kmeans"
        else DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean").fit(datas)
    )

    labels = db.labels_
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print(f"=== 1d clustering for {usg_data} ===")
    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Estimated number of points: %d" % len(datas))

    requests = requests[["IP", "UA"]]
    requests["Label"] = labels
    requests[f"{usg_data}"] = datas
    requests.to_csv("./dumps/1d_{algorithm}_labeling.csv", index=False)

    sort = requests.sort_values(by=f"{usg_data}", ascending=False)
    sort[:100].to_csv(f"./dumps/top_most_{usg_data}_1d.csv", index=False)

    sort = requests.sort_values(by=f"{usg_data}", ascending=True)
    sort[:100].to_csv(f"./dumps/top_least_{usg_data}_1d.csv", index=False)

    # Draw
    # plt.figure(figsize=(12, 7))
    plt.xticks(np.arange(-1, n_clusters_, step=1))
    plt.plot(labels, datas, "o-r", alpha=0.7, lw=0, mec="b", mew=1, ms=10)

    plt.grid(True)
    plt.xlabel("Clusters №")
    plt.ylabel(f"{usg_data}")
    plt.title("Estimated number of clusters: %d" % n_clusters_)
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cluster using DBSCAN or KMeans algorithm."
    )
    parser.add_argument(
        "--input",
        metavar="i",
        type=str,
        help="CSV file with RPI and Deviation columns",
        default="./dumps/requests.csv",
    )
    parser.add_argument(
        "--algorithm",
        dest="alg",
        metavar="a",
        type=str,
        help="The clustering algorithm. Available: kmeans, dbscan",
        default="kmeans",
    )
    parser.add_argument(
        "--dimensionality",
        dest="dim",
        metavar="d",
        type=int,
        help="Data dimension for clustering. Available: 1, 2",
        default=2,
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
        "--one-date",
        dest="one_dim_date",
        metavar="1d",
        type=str,
        help="Data from CSV for 1D clustering. Available: RPI{},Deviation,Mean",
        default="Deviation",
    )
    parser.add_argument(
        "--two-date",
        dest="two_dim_date",
        metavar="2d",
        type=str,
        help="Second data from CSV for 2D clustering. Available: Deviation,Mean",
        default="Deviation",
    )
    parser.add_argument(
        "--min-req",
        dest="min_req",
        metavar="mr",
        type=int,
        help="Minimal amount of requests done by client in one session to be considered for clustering",
        default=4,
    )
    parser.add_argument(
        "--eps",
        dest="eps",
        metavar="e",
        type=int,
        help="Epsilon parameter for dbscan",
        default=10,
    )
    parser.add_argument(
        "--min-samples",
        dest="min_samples",
        metavar="ms",
        type=int,
        help="Min samples parameter for dbscan",
        default=10,
    )
    parser.add_argument(
        "--n_clusters",
        dest="n_clusters",
        metavar="nc",
        type=int,
        help="Number of clusters for kmeans",
        default=7,
    )
    args = parser.parse_args()
    if args.dim == 1:
        one_dim_clustering(
            args.input,
            args.alg,
            args.one_dim_date,
            args.int_len,
            args.min_req,
            args.eps,
            args.min_samples,
            args.n_clusters,
        )
    elif args.dim == 2:
        two_dim_clustering(
            args.input,
            args.alg,
            args.two_dim_date,
            args.int_len,
            args.min_req,
            args.eps,
            args.min_samples,
            args.n_clusters,
        )
    else:
        one_dim_clustering(
            args.input,
            args.alg,
            args.one_dim_date,
            args.int_len,
            args.min_req,
            args.eps,
            args.min_samples,
            args.n_clusters,
        )
        two_dim_clustering(
            args.input,
            args.alg,
            args.two_dim_date,
            args.int_len,
            args.min_req,
            args.eps,
            args.min_samples,
            args.n_clusters,
        )
