"""A sample script which uses data from cli137 on OLCF."""
import os

from ILAMB.ModelResult import ModelResult
from ILAMB.Regions import Regions

from ilambk.pre import install_scripts, prepare_cluster

# register ILAMB regions which exlcude oceans and Antarctica
ROOT = os.environ["ILAMB_ROOT"]
ilamb_region = Regions()
ilamb_region.addRegionLatLonBounds(
    "noant", "No Antarctica", (-60, 89.999), (-179.999, 179.999), ""
)
ilamb_region.addRegionNetCDF4(f"{ROOT}/DATA/regions/GlobalLand.nc")

# setup models to be used
M = []
M.append(ModelResult(f"{ROOT}/MODELS/CMIP6/CESM2", modelname="CESM2"))
M.append(
    ModelResult(
        "",
        modelname="Reference",
        paths=[f"{ROOT}/DATA/pr/GPCPv2.3", f"{ROOT}/DATA/tas/CRU4.02"],
    )
)

# setup clustering
prepare_cluster(
    "sample",
    M,
    [{"vars": ["tas", "pr"], "variability": False}],
    range(1930, 2011, 10),
    regions=["global", "noant"],
)

# copy runtime scripts
install_scripts("sample", "/ccs/home/jbk/bin")
