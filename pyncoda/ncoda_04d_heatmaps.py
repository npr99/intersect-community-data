"""
Comparable heat maps from point data.

Part of the 2010 vs 2020 comparison work (#144): functions that take any
dataframe of point locations (a person file, a housing file, any CSV with
coordinates) and produce kernel density heat maps that can be compared -
two panels on one shared grid, with similarity statistics, and an optional
Folium map for interactive review.

The KDE parameters default to the hot spot analysis conventions
(400 m cells, 500 m bandwidth). Two datasets compared on the same grid
yield two honest numbers: the correlation of their density surfaces, and
the overlap of their top-decile hotspot cells. Those turned "the maps look
similar" into r = 0.89 and 66 percent on the Grays Harbor 2010 vs 2020
comparison that motivated this module.
"""

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter

import folium as fm
from folium.plugins import HeatMap


def people_density_surface(df, lon_col: str = 'x', lat_col: str = 'y',
                           cell_size_m: float = 400.0,
                           window_radius_m: float = 500.0,
                           bounds=None):
    """
    Kernel density surface for one set of points.

    Coordinates are decimal degrees; the grid is built in meters with a
    local equirectangular projection at the points' median latitude.
    bounds, when given as (xmin, xmax, ymin, ymax) in projected meters,
    forces the grid - pass the same bounds to every dataset being
    compared, or use compare_heatmaps which does this automatically.

    Returns (surface, extent_km, bounds_m): the smoothed grid (x by y),
    the plotting extent in kilometers, and the bounds to reuse.
    """

    points = df[[lon_col, lat_col]].dropna()
    # The projection latitude must be shared by every dataset on the grid,
    # so it travels inside bounds: (xmin, xmax, ymin, ymax, lat0). Computing
    # it per dataset would project the two panels differently - a defect the
    # harness check against the recorded Grays Harbor comparison caught.
    if bounds is None:
        lat0 = points[lat_col].median()
    else:
        lat0 = bounds[4]
    mx = 111320.0 * np.cos(np.radians(lat0))
    my = 110540.0
    x = points[lon_col].values * mx
    y = points[lat_col].values * my

    if bounds is None:
        pad = 5.0 * cell_size_m
        bounds = (x.min() - pad, x.max() + pad, y.min() - pad, y.max() + pad,
                  lat0)
    xmin, xmax, ymin, ymax = bounds[:4]
    x_edges = np.arange(xmin, xmax + cell_size_m, cell_size_m)
    y_edges = np.arange(ymin, ymax + cell_size_m, cell_size_m)

    histogram, _, _ = np.histogram2d(x, y, bins=[x_edges, y_edges])
    surface = gaussian_filter(histogram, sigma=window_radius_m / cell_size_m)
    extent_km = [xmin / 1000, xmax / 1000, ymin / 1000, ymax / 1000]
    return surface, extent_km, bounds


def surface_similarity(surface_a, surface_b, hotspot_percentile: float = 90.0,
                       support_epsilon: float = 1e-6):
    """
    How alike two density surfaces on the same grid are.

    Returns a dict: 'correlation' (Pearson r over cells where either
    surface has support) and 'top_decile_jaccard' (share of hotspot cells
    - those at or above hotspot_percentile of each surface's positive
    cells - that the two surfaces have in common).

    support_epsilon sets the density below which a cell does not count as
    positive. The Gaussian smoothing smears infinitesimal density many
    bandwidths from any actual point; a looser epsilon admits those cells
    into the percentile base and inflates the hotspot count. The default
    matches the published Grays Harbor comparison figures.
    """

    support = (surface_a > support_epsilon) | (surface_b > support_epsilon)
    correlation = float(np.corrcoef(surface_a[support],
                                    surface_b[support])[0, 1])

    def hotspot_cells(surface):
        positive = surface[surface > support_epsilon]
        return surface >= np.percentile(positive, hotspot_percentile)

    top_a, top_b = hotspot_cells(surface_a), hotspot_cells(surface_b)
    jaccard = float((top_a & top_b).sum() / (top_a | top_b).sum())
    return {'correlation': correlation, 'top_decile_jaccard': jaccard}


def compare_heatmaps(df_a, df_b, label_a: str, label_b: str,
                     title: str, output_png: str,
                     lon_col: str = 'x', lat_col: str = 'y',
                     cell_size_m: float = 400.0,
                     window_radius_m: float = 500.0,
                     contour_color: str = '#7B4FBF',
                     place_labels=None, dpi: int = 150):
    """
    Two datasets, one shared grid, one figure, two honest numbers.

    Builds both density surfaces on a common grid spanning both point
    sets, draws them side by side with a shared color scale, contours and
    the raw points, and stamps the similarity statistics into the title.
    place_labels, when given as a dataframe with columns
    ['name', lon_col, lat_col], annotates named places on both panels.

    Returns the surface_similarity dict, with the subpopulation counts
    added as 'n_a' and 'n_b'. Saves the figure to output_png.
    """

    both = pd.concat([df_a[[lon_col, lat_col]], df_b[[lon_col, lat_col]]])
    _, _, bounds = people_density_surface(
        both, lon_col, lat_col, cell_size_m, window_radius_m)
    surface_a, extent_km, _ = people_density_surface(
        df_a, lon_col, lat_col, cell_size_m, window_radius_m, bounds)
    surface_b, _, _ = people_density_surface(
        df_b, lon_col, lat_col, cell_size_m, window_radius_m, bounds)

    stats = surface_similarity(surface_a, surface_b)
    stats['n_a'], stats['n_b'] = len(df_a), len(df_b)

    lat0 = both[lat_col].median()
    mx = 111320.0 * np.cos(np.radians(lat0)) / 1000.0
    my = 110540.0 / 1000.0

    figure, axes = plt.subplots(1, 2, figsize=(15, 7.6),
                                sharex=True, sharey=True)
    vmax = max(surface_a.max(), surface_b.max())
    panels = ((axes[0], surface_a, df_a, label_a, len(df_a)),
              (axes[1], surface_b, df_b, label_b, len(df_b)))
    for ax, surface, df, label, n in panels:
        ax.imshow(np.power(surface.T / vmax, 0.5), origin='lower',
                  extent=extent_km, cmap='Purples', vmin=0, vmax=1,
                  aspect='equal')
        ax.contour(surface.T, levels=np.linspace(vmax * 0.15, vmax, 6),
                   origin='lower', extent=extent_km,
                   colors=contour_color, linewidths=0.7)
        pts = df[[lon_col, lat_col]].dropna()
        ax.scatter(pts[lon_col] * mx, pts[lat_col] * my,
                   s=1.5, c='#2d1b4d', alpha=0.3)
        if place_labels is not None:
            for _, row in place_labels.iterrows():
                px, py = row[lon_col] * mx, row[lat_col] * my
                ax.annotate(row['name'], (px, py), fontsize=8,
                            color='#444444', xytext=(0, 7),
                            textcoords='offset points', ha='center')
                ax.plot(px, py, marker='+', ms=5, c='#888888', mew=0.8)
        ax.set_title(f'{label}   (n = {n:,})', fontsize=12)
        ax.set_xticks([])
        ax.set_yticks([])
        bar_x, bar_y = extent_km[0] + 4, extent_km[2] + 4
        ax.plot([bar_x, bar_x + 10], [bar_y, bar_y], c='black', lw=2)
        ax.annotate('10 km', (bar_x + 5, bar_y), xytext=(0, 5),
                    textcoords='offset points', ha='center', fontsize=8)

    figure.suptitle(
        f'{title}\nSame KDE grid ({cell_size_m:.0f} m cells, '
        f'{window_radius_m:.0f} m bandwidth) - surface correlation '
        f'r = {stats["correlation"]:.2f}, top-decile hotspot overlap = '
        f'{stats["top_decile_jaccard"]:.0%}', fontsize=13)
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    figure.savefig(output_png, dpi=dpi, bbox_inches='tight')
    plt.close(figure)
    return stats


def folium_comparison_map(df_a, df_b, label_a: str, label_b: str,
                          output_html: str,
                          lon_col: str = 'x', lat_col: str = 'y',
                          window_radius_px: int = 18):
    """
    Interactive comparison: both datasets as toggleable heat layers.

    Each dataset becomes a Folium HeatMap layer with a layer control, so
    the two can be flicked between or overlaid in the browser. Point
    counts appear in the layer names. Saves to output_html and returns
    the map object.
    """

    both = pd.concat([df_a[[lon_col, lat_col]], df_b[[lon_col, lat_col]]]).dropna()
    center = [both[lat_col].median(), both[lon_col].median()]
    comparison_map = fm.Map(location=center, zoom_start=10,
                            tiles='cartodbpositron')
    for df, label in ((df_a, label_a), (df_b, label_b)):
        pts = df[[lat_col, lon_col]].dropna().values.tolist()
        HeatMap(pts, radius=window_radius_px,
                name=f'{label} (n = {len(df):,})').add_to(comparison_map)
    fm.LayerControl(collapsed=False).add_to(comparison_map)
    comparison_map.save(output_html)
    return comparison_map


def overlay_heatmap(df_a, df_b, label_a: str, label_b: str,
                    title: str, output_png: str,
                    lon_col: str = 'x', lat_col: str = 'y',
                    cell_size_m: float = 200.0,
                    window_radius_m: float = 300.0,
                    color_a: str = '#7B4FBF', color_b: str = '#E66101',
                    zoom_to=None, zoom_pad_m: float = 1500.0,
                    place_labels=None, dpi: int = 150):
    """
    Both groups on ONE map, in contrasting colors.

    Purple for group A and orange for group B follow the ColorBrewer PuOr
    diverging scheme, which is colorblind safe, print friendly and
    photocopy safe. Each group's contours are drawn at fixed fractions of
    that group's own peak density, so the map shows WHERE each group
    concentrates rather than which group is larger; the point counts are
    in the legend for scale.

    zoom_to, when given as a dataframe of points (for example every person
    in one city), restricts the map to that area plus zoom_pad_m on each
    side - at city scale the spatial clustering becomes legible, which
    county-wide maps blur. Points outside the area are dropped before the
    density is computed. The defaults (200 m cells, 300 m bandwidth) suit
    a city; use 400 m and 500 m for a county.

    Returns a dict with the point counts drawn for each group.
    """

    pts_a = df_a[[lon_col, lat_col]].dropna()
    pts_b = df_b[[lon_col, lat_col]].dropna()
    if zoom_to is not None:
        area = zoom_to[[lon_col, lat_col]].dropna()
        lat0 = area[lat_col].median()
        mx = 111320.0 * np.cos(np.radians(lat0))
        my = 110540.0
        pad_lon, pad_lat = zoom_pad_m / mx, zoom_pad_m / my
        lon_min, lon_max = area[lon_col].min() - pad_lon, area[lon_col].max() + pad_lon
        lat_min, lat_max = area[lat_col].min() - pad_lat, area[lat_col].max() + pad_lat
        inside = lambda p: p[(p[lon_col].between(lon_min, lon_max))
                             & (p[lat_col].between(lat_min, lat_max))]
        pts_a, pts_b = inside(pts_a), inside(pts_b)
        frame_for_bounds = pd.DataFrame({lon_col: [lon_min, lon_max],
                                         lat_col: [lat_min, lat_max]})
    else:
        frame_for_bounds = pd.concat([pts_a, pts_b])

    _, extent_km, bounds = people_density_surface(
        frame_for_bounds, lon_col, lat_col, cell_size_m, window_radius_m)
    surface_a, _, _ = people_density_surface(
        pts_a, lon_col, lat_col, cell_size_m, window_radius_m, bounds)
    surface_b, _, _ = people_density_surface(
        pts_b, lon_col, lat_col, cell_size_m, window_radius_m, bounds)

    lat0 = bounds[4]
    mx = 111320.0 * np.cos(np.radians(lat0)) / 1000.0
    my = 110540.0 / 1000.0

    figure, ax = plt.subplots(figsize=(11, 9))
    for surface, pts, color, label in ((surface_a, pts_a, color_a, label_a),
                                       (surface_b, pts_b, color_b, label_b)):
        peak = surface.max()
        if peak > 0:
            ax.contourf(surface.T, levels=np.linspace(peak * 0.25, peak, 5),
                        origin='lower', extent=extent_km,
                        colors=[color], alpha=0.18)
            ax.contour(surface.T, levels=np.linspace(peak * 0.25, peak, 5),
                       origin='lower', extent=extent_km,
                       colors=color, linewidths=0.9)
        ax.scatter(pts[lon_col] * mx, pts[lat_col] * my, s=6, c=color,
                   alpha=0.55, label=f'{label} (n = {len(pts):,})',
                   edgecolors='none')
    if place_labels is not None:
        for _, row in place_labels.iterrows():
            px, py = row[lon_col] * mx, row[lat_col] * my
            if extent_km[0] <= px <= extent_km[1] and extent_km[2] <= py <= extent_km[3]:
                ax.annotate(row['name'], (px, py), fontsize=9, color='#333333',
                            xytext=(0, 7), textcoords='offset points', ha='center')
                ax.plot(px, py, marker='+', ms=6, c='#666666', mew=0.9)
    ax.set_xlim(extent_km[0], extent_km[1])
    ax.set_ylim(extent_km[2], extent_km[3])
    ax.set_aspect('equal')
    ax.set_xticks([])
    ax.set_yticks([])
    bar_km = 2 if zoom_to is not None else 10
    bar_x, bar_y = extent_km[0] + (extent_km[1] - extent_km[0]) * 0.04,                    extent_km[2] + (extent_km[3] - extent_km[2]) * 0.04
    ax.plot([bar_x, bar_x + bar_km], [bar_y, bar_y], c='black', lw=2)
    ax.annotate(f'{bar_km} km', (bar_x + bar_km / 2, bar_y), xytext=(0, 5),
                textcoords='offset points', ha='center', fontsize=8)
    ax.legend(loc='upper right', frameon=True, fontsize=9)
    subtitle = ('Contours at 25-100% of each group' + chr(39) + 's own peak density '
                f'({cell_size_m:.0f} m cells, {window_radius_m:.0f} m bandwidth)')
    ax.set_title(title + chr(10) + subtitle, fontsize=12)
    plt.tight_layout()
    figure.savefig(output_png, dpi=dpi, bbox_inches='tight')
    plt.close(figure)
    return {'n_a': len(pts_a), 'n_b': len(pts_b)}
