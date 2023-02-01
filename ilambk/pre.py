"""Functions for using ILAMB to prepare data for clustering."""
import os
import pickle
import sys
from typing import List, Union

import numpy as np
from ILAMB.ModelResult import ModelResult
from ILAMB.Regions import Regions

if sys.version_info > (3, 9):
    from importlib.resources import files
else:
    from importlib_resources import files


__all__ = ["prepare_cluster"]


def prepare_cluster(
    casename: str,
    models: Union[ModelResult, List[ModelResult]],
    variables: List[dict],
    times: List,
    regions: Union[str, List[str]] = None,
    verbose: bool = False,
):
    """."""
    # Check types
    if isinstance(models, ModelResult):
        models = [models]
    for mod in models:
        assert isinstance(mod, ModelResult)
    if regions is None:
        regions = ["global"]
    if isinstance(regions, str):
        regions = [regions]

    # Setup
    years = np.asarray(times)[:-1]
    times = (np.asarray(times, dtype=float) - 1850) * 365
    pathname = os.path.join(casename, "data")
    if not os.path.isdir(pathname):
        os.makedirs(pathname)
    ilamb_region = Regions()

    # Improve this
    pref_units = {
        "pr": "mm d-1",
        "gpp": "g m-2 d-1",
        "tas": "degC",
        "tasmin": "degC",
        "tasmax": "degC",
        "tasmaxQ10": "degC",
        "tasmaxQ30": "degC",
        "tasmaxQ50": "degC",
        "tasmaxQ70": "degC",
        "tasmaxQ90": "degC",
    }

    # Main loop which builds up input data and auxillary arrays
    row = 0
    data = []
    stack = []
    for mod in models:
        lat = lon = area = mask = names = None
        if verbose:
            print(f"Processing {mod.name}...")
        for i, year in enumerate(years):
            # This complete is used to accelerate moving on if any particular
            # variable is not present in a model.
            complete = True
            columns = {}
            for _, dct in enumerate(variables):
                compute_mean = dct["mean"] if "mean" in dct else True
                compute_var = dct["variability"] if "variability" in dct else False
                for vname in dct["vars"]:
                    if not complete:
                        continue
                    # Extract the variable from the model, we require that 90%
                    # of the input interval be present to count in this timespan
                    try:
                        var = mod.extractTimeSeries(
                            vname, initial_time=times[i], final_time=times[i + 1]
                        )
                        var.trim(t=times[i : (i + 2)])
                        if (var.time_bnds[-1, 1] - var.time_bnds[0, 0]) / (
                            times[i + 1] - times[i]
                        ) < 0.9:
                            complete = False
                            continue
                        if vname in pref_units:
                            var.convert(pref_units[vname])
                    # pylint: disable=bare-except
                    except:
                        # if any of the above failed for any reason, this
                        # model/decade cannot be part of this clustering.
                        complete = False
                        continue
                    # What aspects of this variable do we want to add as columns?
                    if compute_mean:
                        lbl = f"mean({vname}) [{var.unit}]"
                        columns[lbl] = var.integrateInTime(mean=True)
                    if compute_var:
                        lbl = f"std({vname}) [{var.unit}]"
                        columns[lbl] = var.variability()
            if not complete:
                continue

            # All variables must have values at all grid cells for this
            # model/timespan. Build up a composite mask and apply across all
            # variables in this column.
            if mask is None:
                for _, var in columns.items():
                    if mask is None:
                        mask = var.data.mask
                    mask += var.data.mask
            for region in regions:
                mask += ilamb_region.getMask(region, var)
            data.append(
                np.vstack(
                    [
                        np.ma.masked_array(var.data, mask=mask).compressed()
                        for _, var in columns.items()
                    ]
                )
            )
            stack.append(f"{mod.name} {year} {row}")

            # Build and write coordinate values for this model
            lat, lon = np.meshgrid(var.lat, var.lon, indexing="ij")
            lat = np.ma.masked_array(lat, mask=mask).compressed()
            lon = np.ma.masked_array(lon, mask=mask).compressed()
            area = np.ma.masked_array(var.area, mask=mask).compressed()
            row += lat.size
            names = list(columns.keys())
            np.savetxt(
                os.path.join(pathname, f"coords.{mod.name}"),
                np.vstack([lon, lat, area]).T,
                delimiter=" ",
            )

    # Write files
    np.savetxt(
        os.path.join(pathname, f"obs.raw.{casename}"), np.hstack(data).T, delimiter=" "
    )
    with open(
        os.path.join(pathname, f"stack.{casename}"), encoding="UTF-8", mode="w"
    ) as file:
        file.write("\n".join(stack))
    with open(os.path.join(pathname, f"names.{casename}"), mode="wb") as file:
        pickle.dump(names, file)


def install_scripts(casename: str, cluster_bin: str):
    """."""

    content = files("ilambk").joinpath("job.sh").read_text(encoding="UTF-8")
    content = content.replace("CASENAME", casename)
    content = content.replace(
        "DATALOCATION", os.path.join(os.getcwd(), casename, "data")
    )
    content = content.replace("CLUSTERBIN", cluster_bin)
    if not os.path.isdir(casename):
        os.makedirs(casename)
    with open(os.path.join(casename, "job.sh"), encoding="UTF-8", mode="w") as file:
        file.write(content)
    with open(
        os.path.join(casename, "run_clustering.sh"), encoding="UTF-8", mode="w"
    ) as file:
        file.write(
            files("ilambk").joinpath("run_clustering.sh").read_text(encoding="UTF-8")
        )
