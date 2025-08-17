from dataclasses import dataclass
from typing import Optional, Sequence

@dataclass(frozen=True)
class VarSpec:
    short_names: Sequence[str]
    type_of_level: Optional[Sequence[str]]
    level: Optional[int]
    contains_any: Sequence[str] = ()

VARIABLES = {
    "temperature_2m": VarSpec(
        short_names=["TMP", "t", "unknown"],
        type_of_level=["heightAboveGround"],
        level=2,
        contains_any=("2 metre temperature", "temperature: k", "2 m above ground")
    ),

    "surface_pressure": VarSpec(
        short_names=["PRES", "mslma", "PRMSL", "prmsl"],
        type_of_level=["surface", "meanSea"],
        level=0,
        contains_any=("surface pressure", "mslp", "mean sea")
    ),

    "u_component_wind_80m": VarSpec(
        short_names=["u", "UGRD"],
        type_of_level=["heightAboveGround"],
        level=80,
        contains_any=(" 80 m", "80 metre", "u component of wind")
    ),
    "v_component_wind_80m": VarSpec(
        short_names=["v", "VGRD"],
        type_of_level=["heightAboveGround"],
        level=80,
        contains_any=(" 80 m", "80 metre", "v component of wind")
    ),

    # >>> broadened hints for your f02 dump <<<
    "u_component_wind_10m": VarSpec(
        short_names=["u", "UGRD", "10u", "unknown"],
        type_of_level=["heightAboveGround"],
        level=10,
        contains_any=("10 metre u wind component", "10 m above ground", "u component of wind")
    ),
    "v_component_wind_10m": VarSpec(
        short_names=["v", "VGRD", "10v", "unknown"],
        type_of_level=["heightAboveGround"],
        level=10,
        contains_any=("10 metre v wind component", "10 m above ground", "v component of wind")
    ),

    "dewpoint_2m": VarSpec(
        short_names=["DPT", "dpt", "unknown"],
        type_of_level=["heightAboveGround"],
        level=2,
        contains_any=("2 metre dewpoint", "dewpoint temperature", "dew point", "2 m above ground")
    ),
    "relative_humidity_2m": VarSpec(
        short_names=["RH", "r", "unknown"],
        type_of_level=["heightAboveGround"],
        level=2,
        contains_any=("2 metre relative humidity", "relative humidity", "2 m above ground")
    ),

    "surface_roughness": VarSpec(
        short_names=["SFCR", "fricv", "unknown"],
        type_of_level=["surface"],
        level=0,
        contains_any=("surface roughness", "sfcr:surface")
    ),

    "visible_beam_downward_solar_flux": VarSpec(
        short_names=["VBDSF", "vbdsf", "unknown"],
        type_of_level=["surface"],
        level=0,
        contains_any=("visible beam downward solar flux", "vbdsf:surface")
    ),
    "visible_diffuse_downward_solar_flux": VarSpec(
        short_names=["VDDSF", "vddsf", "unknown"],
        type_of_level=["surface"],
        level=0,
        contains_any=("visible diffuse downward solar flux", "vddsf:surface")
    ),
}

ALL_SUPPORTED = list(VARIABLES.keys())