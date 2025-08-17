import numpy as np

def haversine_min_idx(latlon_grid, target_lat, target_lon):
    lats, lons = latlon_grid
    # radians
    lat1 = np.radians(lats)
    lon1 = np.radians(lons)
    lat2 = np.radians(target_lat)
    lon2 = np.radians(target_lon)
    dlat = lat1 - lat2
    dlon = lon1 - lon2
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    d = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    idx = np.unravel_index(np.argmin(d), d.shape)
    return idx
