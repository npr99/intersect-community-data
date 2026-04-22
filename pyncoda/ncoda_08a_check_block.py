import warnings

import contextily as cx
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from pyncoda.ncoda_00h_bldg_archetype_structure import HAZUS_residential_archetypes


class BlockValidationChecker:
	"""Validate HUA results for one block using Building, ADDPT, and HUA comparisons."""

	def __init__(
		self,
		hua_gdf,
		tabblock_gdf,
		nsi_gdf,
		addpt_df=None,
		*,
		block_col="blockid",
		block_col_tab="blockid",
		bldg_uniqueid="fd_id_bid",
		addpt_structure_id="strctid",
		addpt_occtype_col="occtype",
		bldg_archetype_col="occtype",
		hua_unit_col="huid",
		missing_building_col="Building Data Availability_str",
		missing_building_label="0 Missing Building Data",
		default_expected_units=1,
		buffer_m=100,
		wgs84_epsg=4326,
		metric_epsg=3857,
		residential_archetypes=None,
	):
		self.block_col = block_col
		self.block_col_tab = block_col_tab
		self.bldg_uniqueid = bldg_uniqueid
		self.addpt_structure_id = addpt_structure_id
		self.addpt_occtype_col = addpt_occtype_col
		self.bldg_archetype_col = bldg_archetype_col
		self.hua_unit_col = hua_unit_col
		self.missing_building_col = missing_building_col
		self.missing_building_label = missing_building_label
		self.default_expected_units = default_expected_units
		self.buffer_m = buffer_m
		self.wgs84_epsg = wgs84_epsg
		self.metric_epsg = metric_epsg
		self.residential_archetypes = residential_archetypes or HAZUS_residential_archetypes

		self.hua_gdf = self._to_geodataframe(hua_gdf, name="hua_gdf")
		self.tabblock_gdf = self._to_geodataframe(tabblock_gdf, name="tabblock_gdf")
		self.nsi_gdf = self._to_geodataframe(nsi_gdf, name="nsi_gdf")
		self.addpt_gdf = self._to_geodataframe(addpt_df, name="addpt_df", allow_none=True)

		self._validate_required_columns()
		self._normalize_crs()

	def _to_geodataframe(self, df, *, name, allow_none=False):
		if df is None:
			if allow_none:
				return None
			raise ValueError(f"{name} is required.")

		if isinstance(df, gpd.GeoDataFrame):
			return df.copy(deep=True)

		if isinstance(df, pd.DataFrame):
			copy_df = df.copy(deep=True)
			if "geometry" in copy_df.columns:
				geom_sample = copy_df["geometry"].dropna().head(1)
				if not geom_sample.empty and isinstance(geom_sample.iloc[0], str):
					# Support CSV inputs where geometry is stored as WKT strings (e.g., "POINT (...)" ).
					copy_df["geometry"] = gpd.GeoSeries.from_wkt(copy_df["geometry"])
				gdf = gpd.GeoDataFrame(copy_df, geometry="geometry", crs=f"epsg:{self.wgs84_epsg}")
				return gdf
			if all(col in copy_df.columns for col in ["x", "y"]):
				gdf = gpd.GeoDataFrame(
					copy_df,
					geometry=gpd.points_from_xy(copy_df["x"], copy_df["y"]),
					crs=f"epsg:{self.wgs84_epsg}",
				)
				return gdf

		raise ValueError(
			f"{name} must be a GeoDataFrame or a DataFrame with geometry or x/y columns."
		)

	def _validate_required_columns(self):
		self._require_columns(self.hua_gdf, [self.block_col, self.bldg_uniqueid], "hua_gdf")
		self._require_columns(self.tabblock_gdf, [self.block_col_tab], "tabblock_gdf")
		self._require_columns(self.nsi_gdf, [self.bldg_uniqueid, self.bldg_archetype_col], "nsi_gdf")
		if self.addpt_gdf is not None:
			self._require_columns(
				self.addpt_gdf,
				[self.addpt_structure_id, self.bldg_uniqueid, self.addpt_occtype_col],
				"addpt_df",
			)

	@staticmethod
	def _require_columns(df, required_columns, df_name):
		missing = [col for col in required_columns if col not in df.columns]
		if missing:
			raise ValueError(f"{df_name} is missing required columns: {missing}")

	def _normalize_crs(self):
		self.hua_gdf = self._ensure_crs(self.hua_gdf, self.wgs84_epsg)
		self.tabblock_gdf = self._ensure_crs(self.tabblock_gdf, self.wgs84_epsg)
		self.nsi_gdf = self._ensure_crs(self.nsi_gdf, self.wgs84_epsg)
		if self.addpt_gdf is not None:
			self.addpt_gdf = self._ensure_crs(self.addpt_gdf, self.wgs84_epsg)

	@staticmethod
	def _ensure_crs(gdf, target_epsg):
		if gdf.crs is None:
			gdf = gdf.set_crs(epsg=target_epsg, allow_override=True)
		return gdf.to_crs(epsg=target_epsg)

	def _block_id_as_str(self, block_id):
		return str(block_id)

	@staticmethod
	def _normalize_blockid_series(series, width=15):
		"""Normalize block IDs to digit-only strings padded to a common width."""
		if series is None:
			return series
		normalized = (
			series.astype(str)
			.str.replace(r"^B", "", regex=True)
			.str.replace(r"\.0$", "", regex=True)
			.str.replace(r"\D", "", regex=True)
		)
		return normalized.str.zfill(width)

	def validate_allocation_by_block(self, population_col="numprec"):
		"""Summarize HUA allocation quality by block.

		Returns one row per block with:
		- housing unit counts
		- population totals (if available)
		- missing-building counts/rates
		- address point totals (if available)
		"""
		hua = self.hua_gdf.copy(deep=True)
		hua["_blockid"] = self._normalize_blockid_series(hua[self.block_col])

		agg_spec = {
			"housing_units": (self.hua_unit_col, "nunique"),
			"housing_unit_records": (self.hua_unit_col, "size"),
		}
		if population_col in hua.columns:
			agg_spec["total_population"] = (population_col, "sum")
		else:
			warnings.warn(
				f"Population column {population_col} not found. total_population set to NA."
			)

		block_summary = hua.groupby("_blockid", dropna=False).agg(**agg_spec).reset_index()
		block_summary = block_summary.rename(columns={"_blockid": self.block_col})

		if "total_population" not in block_summary.columns:
			block_summary["total_population"] = pd.NA

		if self.missing_building_col in hua.columns:
			missing_condition = hua[self.missing_building_col] == self.missing_building_label
			missing_counts = (
				hua[missing_condition]
				.groupby("_blockid", dropna=False)
				.size()
				.reset_index(name="missing_building_hu")
				.rename(columns={"_blockid": self.block_col})
			)
			block_summary = block_summary.merge(missing_counts, on=self.block_col, how="left")
		else:
			block_summary["missing_building_hu"] = pd.NA

		block_summary["missing_building_hu"] = block_summary["missing_building_hu"].fillna(0)
		block_summary["missing_building_pct"] = (
			block_summary["missing_building_hu"]
			/ block_summary["housing_units"].replace({0: pd.NA})
		)

		if self.addpt_gdf is not None:
			addpt = self.addpt_gdf.copy(deep=True)
			if self.block_col in addpt.columns:
				addpt["_blockid"] = self._normalize_blockid_series(addpt[self.block_col])
			elif "BLOCKID10_str" in addpt.columns:
				addpt["_blockid"] = self._normalize_blockid_series(addpt["BLOCKID10_str"])
			elif "BLOCKID20_str" in addpt.columns:
				addpt["_blockid"] = self._normalize_blockid_series(addpt["BLOCKID20_str"])
			else:
				addpt["_blockid"] = pd.NA

			if addpt["_blockid"].notna().any():
				addpt_counts = (
					addpt[addpt["_blockid"].notna()]
					.groupby("_blockid", dropna=False)
					.size()
					.reset_index(name="addpt_points")
					.rename(columns={"_blockid": self.block_col})
				)
				block_summary = block_summary.merge(addpt_counts, on=self.block_col, how="left")
			else:
				block_summary["addpt_points"] = pd.NA
		else:
			block_summary["addpt_points"] = pd.NA

		block_summary["addpt_points"] = block_summary["addpt_points"].fillna(0)
		block_summary["allocation_priority_score"] = (
			block_summary["addpt_points"].astype(float)
			+ block_summary["total_population"].fillna(0).astype(float)
		)

		return block_summary.sort_values(
			by=["missing_building_hu", "addpt_points", "total_population"],
			ascending=[False, False, False],
		)

	def identify_blocks_with_missing_building_data(self):
		"""Backward-compatible wrapper returning missing-building counts by block."""
		block_summary = self.validate_allocation_by_block()
		return block_summary.set_index(self.block_col)["missing_building_hu"].sort_values(ascending=False)

	def prepare_block_context(self, block_id):
		block_id_str = self._normalize_blockid_series(pd.Series([block_id])).iloc[0]

		tab = self.tabblock_gdf.copy(deep=True)
		tab["_blockid"] = self._normalize_blockid_series(tab[self.block_col_tab])
		block_polygon_gdf = tab[tab["_blockid"] == block_id_str].copy(deep=True)
		if block_polygon_gdf.empty:
			raise ValueError(f"No polygon found in tabblock_gdf for block_id {block_id_str}")

		hua = self.hua_gdf.copy(deep=True)
		hua["_blockid"] = self._normalize_blockid_series(hua[self.block_col])
		block_hua_gdf = hua[hua["_blockid"] == block_id_str].copy(deep=True)

		block_polygon_3857 = block_polygon_gdf.to_crs(epsg=self.metric_epsg)
		block_buffer_3857 = gpd.GeoDataFrame(
			geometry=block_polygon_3857.buffer(self.buffer_m),
			crs=f"epsg:{self.metric_epsg}",
		)
		block_polygon_union = block_polygon_3857.geometry.union_all()
		block_buffer_union = block_buffer_3857.geometry.union_all()

		nsi_3857 = self.nsi_gdf.to_crs(epsg=self.metric_epsg)
		nsi_in_buffer = nsi_3857[nsi_3857.geometry.intersects(block_buffer_union)].copy(deep=True)
		nsi_in_block = nsi_3857[nsi_3857.geometry.intersects(block_polygon_union)].copy(deep=True)

		addpt_in_buffer = None
		addpt_in_block = None
		if self.addpt_gdf is not None:
			addpt_3857 = self.addpt_gdf.to_crs(epsg=self.metric_epsg)
			addpt_in_buffer = addpt_3857[addpt_3857.geometry.intersects(block_buffer_union)].copy(deep=True)
			addpt_in_block = addpt_3857[addpt_3857.geometry.intersects(block_polygon_union)].copy(deep=True)

		return {
			"block_id": block_id_str,
			"block_polygon_gdf": block_polygon_gdf,
			"block_polygon_3857": block_polygon_3857,
			"block_buffer_3857": block_buffer_3857,
			"block_hua_gdf": block_hua_gdf,
			"nsi_in_buffer": nsi_in_buffer,
			"nsi_in_block": nsi_in_block,
			"addpt_in_buffer": addpt_in_buffer,
			"addpt_in_block": addpt_in_block,
		}

	def _archetype_expected_units_lookup(self):
		lookup = {}
		for key, value in self.residential_archetypes.items():
			if isinstance(value, dict):
				lookup[str(key)] = value.get("HU estimate", self.default_expected_units)
			else:
				lookup[str(key)] = self.default_expected_units
		return lookup

	def build_expected_units_from_archetypes(self, block_id):
		context = self.prepare_block_context(block_id)
		nsi_block = context["nsi_in_block"].copy(deep=True)
		addpt_block = context["addpt_in_block"]
		hua_block = context["block_hua_gdf"].to_crs(epsg=self.metric_epsg).copy(deep=True)

		if nsi_block.empty:
			return gpd.GeoDataFrame(columns=[self.bldg_uniqueid, "expected_units", "geometry"])

		lookup = self._archetype_expected_units_lookup()
		nsi_block["_archetype_str"] = nsi_block[self.bldg_archetype_col].astype(str)
		nsi_block["expected_units"] = (
			nsi_block["_archetype_str"].map(lookup).fillna(self.default_expected_units)
		)
		nsi_block["archetype_unmatched"] = ~nsi_block["_archetype_str"].isin(lookup.keys())

		expected_by_structure = (
			nsi_block.groupby(self.bldg_uniqueid, dropna=False)
			.agg(
				occtype=(self.bldg_archetype_col, lambda s: "|".join(sorted({str(v) for v in s.dropna()}))),
				expected_units=("expected_units", "sum"),
				geometry=("geometry", "first"),
				archetype_unmatched=("archetype_unmatched", "max"),
			)
			.reset_index()
		)
		expected_by_structure = gpd.GeoDataFrame(
			expected_by_structure,
			geometry="geometry",
			crs=nsi_block.crs,
		)

		if addpt_block is not None and not addpt_block.empty:
			addpt_counts = (
				addpt_block.groupby(self.bldg_uniqueid, dropna=False)
				.agg(
					addpt_count=(self.bldg_uniqueid, "size"),
					addpt_occtype=(self.addpt_occtype_col, lambda s: "|".join(sorted({str(v) for v in s.dropna()}))),
					addpt_geometry=("geometry", "first"),
				)
				.reset_index()
			)
		else:
			addpt_counts = pd.DataFrame(
				columns=[self.bldg_uniqueid, "addpt_count", "addpt_occtype", "addpt_geometry"]
			)

		if not hua_block.empty:
			hua_counts = (
				hua_block.groupby(self.bldg_uniqueid, dropna=False)[self.hua_unit_col]
				.nunique()
				.reset_index(name="hua_allocated_units")
			)
			hua_geometry = (
				hua_block.groupby(self.bldg_uniqueid, dropna=False)["geometry"]
				.first()
				.reset_index(name="hua_geometry")
			)
			hua_counts = hua_counts.merge(hua_geometry, on=self.bldg_uniqueid, how="left")
		else:
			hua_counts = pd.DataFrame(columns=[self.bldg_uniqueid, "hua_allocated_units", "hua_geometry"])

		compare_gdf = expected_by_structure.merge(addpt_counts, on=self.bldg_uniqueid, how="outer")
		compare_gdf = compare_gdf.merge(hua_counts, on=self.bldg_uniqueid, how="outer")
		compare_gdf["geometry"] = compare_gdf["geometry"].where(compare_gdf["geometry"].notna(), compare_gdf["addpt_geometry"])
		compare_gdf["geometry"] = compare_gdf["geometry"].where(compare_gdf["geometry"].notna(), compare_gdf["hua_geometry"])
		compare_gdf["occtype"] = compare_gdf["occtype"].where(compare_gdf["occtype"].notna(), compare_gdf["addpt_occtype"])
		compare_gdf = compare_gdf.drop(columns=["addpt_geometry", "hua_geometry", "addpt_occtype"], errors="ignore")
		compare_gdf = gpd.GeoDataFrame(compare_gdf, geometry="geometry", crs=f"epsg:{self.metric_epsg}")
		compare_gdf = compare_gdf[compare_gdf.geometry.notna()].copy(deep=True)
		compare_gdf["addpt_count"] = compare_gdf["addpt_count"].fillna(0)
		compare_gdf["hua_allocated_units"] = compare_gdf["hua_allocated_units"].fillna(0)
		compare_gdf["expected_units"] = compare_gdf["expected_units"].fillna(0)

		compare_gdf["expected_minus_addpt"] = compare_gdf["expected_units"] - compare_gdf["addpt_count"]
		compare_gdf["addpt_minus_hua"] = compare_gdf["addpt_count"] - compare_gdf["hua_allocated_units"]
		compare_gdf["expected_minus_hua"] = (
			compare_gdf["expected_units"] - compare_gdf["hua_allocated_units"]
		)

		return compare_gdf

	def compare_expected_to_addpt_by_occtype(self, block_id):
		"""Compare expected units from archetypes to ADDPT counts by occupancy type."""
		context = self.prepare_block_context(block_id)
		nsi_block = context["nsi_in_block"].copy(deep=True)
		addpt_block = context["addpt_in_block"]

		lookup = self._archetype_expected_units_lookup()
		if not nsi_block.empty:
			nsi_block["_archetype_str"] = nsi_block[self.bldg_archetype_col].astype(str)
			nsi_block["expected_units"] = (
				nsi_block["_archetype_str"].map(lookup).fillna(self.default_expected_units)
			)
			expected_by_occtype = (
				nsi_block.groupby(self.bldg_archetype_col, dropna=False)["expected_units"]
				.sum()
				.reset_index(name="expected_units")
			)
		else:
			expected_by_occtype = pd.DataFrame(
				columns=[self.bldg_archetype_col, "expected_units"]
			)

		if addpt_block is not None and not addpt_block.empty:
			addpt_by_occtype = (
				addpt_block.groupby(self.addpt_occtype_col, dropna=False)
				.size()
				.reset_index(name="addpt_count")
			)
		else:
			addpt_by_occtype = pd.DataFrame(
				columns=[self.addpt_occtype_col, "addpt_count"]
			)

		compare = expected_by_occtype.merge(
			addpt_by_occtype,
			left_on=self.bldg_archetype_col,
			right_on=self.addpt_occtype_col,
			how="outer",
		)
		if self.addpt_occtype_col != self.bldg_archetype_col:
			compare[self.bldg_archetype_col] = compare[self.bldg_archetype_col].fillna(
				compare[self.addpt_occtype_col]
			)
			compare = compare.drop(columns=[self.addpt_occtype_col])
		else:
			compare = compare.rename(columns={self.addpt_occtype_col: self.bldg_archetype_col})

		compare["expected_units"] = compare["expected_units"].fillna(0)
		compare["addpt_count"] = compare["addpt_count"].fillna(0)
		compare["expected_minus_addpt"] = compare["expected_units"] - compare["addpt_count"]

		return compare.sort_values("expected_minus_addpt", ascending=False)

	def flag_underallocation_candidates(self, block_id):
		compare_gdf = self.build_expected_units_from_archetypes(block_id)
		if compare_gdf.empty:
			return compare_gdf

		compare_gdf["possible_addpt_under_generation"] = (
			compare_gdf["addpt_count"] < compare_gdf["expected_units"]
		)
		compare_gdf["possible_hua_under_assignment"] = (
			compare_gdf["hua_allocated_units"] < compare_gdf["addpt_count"]
		)
		compare_gdf["possible_geometry_coverage_gap"] = (
			compare_gdf["hua_allocated_units"] == 0
		) & (compare_gdf["expected_units"] > 0)

		return compare_gdf

	@staticmethod
	def _scale_point_sizes(values, min_size=20, size_per_unit=6, max_size=1200):
		values = pd.Series(values).fillna(0).astype(float)
		if values.empty:
			return values
		sizes = min_size + values * size_per_unit
		return sizes.clip(upper=max_size)

	@staticmethod
	def _plot_base_layers(ax, block_polygon_3857, block_buffer_3857):
		block_polygon_3857.plot(
			ax=ax,
			facecolor="none",
			edgecolor="black",
			linewidth=1.5,
		)
		block_buffer_3857.plot(
			ax=ax,
			facecolor="none",
			edgecolor="gray",
			linewidth=1,
			linestyle="--",
		)

	def plot_three_panel_comparison(self, block_id, figsize=(18, 6), add_basemap=True):
		context = self.prepare_block_context(block_id)
		compare_gdf = self.build_expected_units_from_archetypes(block_id)

		fig, axes = plt.subplots(1, 3, figsize=figsize)

		block_polygon_3857 = context["block_polygon_3857"]
		block_buffer_3857 = context["block_buffer_3857"]

		# Panel 1: Buildings sized by expected units.
		ax = axes[0]
		self._plot_base_layers(ax, block_polygon_3857, block_buffer_3857)
		if not compare_gdf.empty:
			sizes = self._scale_point_sizes(compare_gdf["expected_units"])
			compare_gdf.plot(
				ax=ax,
				markersize=sizes,
				color="tab:blue",
				alpha=0.7,
			)
		ax.set_title("1. Building (Expected Units)")

		# Panel 2: Address points sized by structure-level address point counts.
		ax = axes[1]
		self._plot_base_layers(ax, block_polygon_3857, block_buffer_3857)
		if not compare_gdf.empty:
			sizes = self._scale_point_sizes(compare_gdf["addpt_count"])
			compare_gdf.plot(
				ax=ax,
				markersize=sizes,
				color="tab:orange",
				alpha=0.7,
			)
		ax.set_title("2. ADDPT (Points per Structure)")

		# Panel 3: HUA sized by structure-level allocated units.
		ax = axes[2]
		self._plot_base_layers(ax, block_polygon_3857, block_buffer_3857)
		if not compare_gdf.empty:
			sizes = self._scale_point_sizes(compare_gdf["hua_allocated_units"])
			compare_gdf.plot(
				ax=ax,
				markersize=sizes,
				color="tab:red",
				alpha=0.7,
			)
		ax.set_title("3. HUA (Allocated Units per Structure)")

		for ax in axes:
			if add_basemap:
				try:
					cx.add_basemap(ax, source=cx.providers.CartoDB.PositronNoLabels)
				except Exception:
					warnings.warn("Basemap could not be loaded; plotting without tiles.")
			ax.set_axis_off()

		plt.tight_layout()
		return fig, axes, compare_gdf

