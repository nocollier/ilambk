"""Functions which are used to postprocess the clustering results."""
import os
import pickle

import numpy as np
import pandas as pd

__all__ = ["build_cluster_dataframe", "build_centroid_dataframe"]


def build_cluster_dataframe(casename: str):
    """Walk through the case directory and build a dataframe with the clustering results."""
    # Build up the auxillary information using the stack file and coordinates
    path = os.path.join(casename, "data")
    stack_file = os.path.join(path, f"stack.{casename}")
    if not os.path.isfile(stack_file):
        raise ValueError(f"{stack_file} not found")
    df_stack = pd.read_csv(
        stack_file, sep=" ", header=None, names=["model", "time", "row"]
    )
    df_coord = {}
    for mod in df_stack.model.unique():
        coords_file = os.path.join(path, f"coords.{mod}")
        if not os.path.isfile(coords_file):
            raise ValueError(f"{coords_file} not found")
        df_coord[mod] = pd.read_csv(
            coords_file,
            sep=" ",
            header=None,
            names=["lon", "lat", "area"],
        )
    df_stack["lat"] = [df_coord[m].lat for m in df_stack.model]
    df_stack["lon"] = [df_coord[m].lon for m in df_stack.model]
    df_stack["area"] = [df_coord[m].area for m in df_stack.model]
    df_stack = (
        df_stack.explode(["lat", "lon", "area"]).drop(columns="row").reset_index()
    )
    df_stack["lat"] = df_stack["lat"].astype(float)
    df_stack["lon"] = df_stack["lon"].astype(float)
    df_stack["area"] = df_stack["area"].astype(float)

    # Build up clusters
    df_clusters = {}
    for root, _, files in os.walk(casename):
        for fname in files:
            if fname.startswith("clusters.out."):
                kval = fname.split(".")[-1]
                kval = f"k{kval}"
                df_clusters[kval] = pd.read_csv(
                    os.path.join(root, fname), sep=" ", header=None, names=[kval]
                ).astype({kval: "int"})
                df_clusters[kval] = df_clusters[kval][kval].to_numpy()
    df_clusters = pd.DataFrame(df_clusters)

    df_out = pd.concat([df_stack, df_clusters], axis=1).drop(columns="index")
    return df_out


def build_centroid_dataframe(casename: str):
    """."""
    path = os.path.join(casename, "data")
    name_file = os.path.join(path, f"name.{casename}")
    if not os.path.isfile(name_file):
        raise ValueError(f"{name_file} not found")
    with open(name_file, "rb") as pkl:
        names = pickle.load(pkl)
    dfs = []
    for root, _, files in os.walk(casename):
        for fname in files:
            if fname.startswith("seeds.out") and fname.endswith(".unstd"):
                kval = fname.split(".")[-3]
                kval = f"k{kval}"
                dfc = pd.read_csv(
                    os.path.join(root, fname),
                    sep=" ",
                    header=None,
                    names=[
                        "junk",
                    ]
                    + names,
                ).drop(columns="junk")
                dfc["k"] = kval
                dfs.append(dfc)
    return pd.concat(dfs).reset_index()
