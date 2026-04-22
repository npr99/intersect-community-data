import warnings

import contextily as cx
import folium
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
		self._nsi_metric_cache = None
		self._addpt_metric_cache = None
		self._folium_scale_maxima_cache = None

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

	def _get_metric_layers(self):
		"""Cache metric-CRS layers used repeatedly in block-level scans."""
		if self._nsi_metric_cache is None:
			self._nsi_metric_cache = self.nsi_gdf.to_crs(epsg=self.metric_epsg)
		if self.addpt_gdf is not None and self._addpt_metric_cache is None:
			self._addpt_metric_cache = self.addpt_gdf.to_crs(epsg=self.metric_epsg)
		return self._nsi_metric_cache, self._addpt_metric_cache

	def _hua_plot_group_key(self, hua_df):
		"""Build grouping keys; collapse all missing-building HUA rows to one key."""
		fd_series = hua_df[self.bldg_uniqueid]
		fd_as_str = fd_series.astype(str)
		missing_mask = fd_series.isna() | (fd_as_str.str.strip().str.lower() == "missing building id")
		group_key = fd_as_str.where(~missing_mask, "missing building id")
		return group_key, missing_mask

	def _get_folium_scale_maxima(self):
		"""Return dataset-wide maxima so marker sizes are comparable across maps."""
		cache = getattr(self, "_folium_scale_maxima_cache", None)
		if cache is not None:
			return cache

		# NSI expected units by archetype
		expected_lookup = self._archetype_expected_units_lookup()
		nsi_expected = (
			self.nsi_gdf[self.bldg_archetype_col]
			.astype(str)
			.map(expected_lookup)
			.fillna(self.default_expected_units)
		)
		max_expected = float(nsi_expected.max()) if len(nsi_expected) else 1.0

		# ADDPT points per structure
		if self.addpt_gdf is not None and not self.addpt_gdf.empty and self.bldg_uniqueid in self.addpt_gdf.columns:
			addpt_counts = self.addpt_gdf.groupby(self.bldg_uniqueid, dropna=False).size()
			max_addpt = float(addpt_counts.max()) if len(addpt_counts) else 1.0
		else:
			max_addpt = 1.0

		# HUA allocated units per plotted HUA point.
		hua_for_scale = self.hua_gdf.copy(deep=True)
		if not hua_for_scale.empty and self.hua_unit_col in hua_for_scale.columns:
			hua_for_scale["_hua_group_key"], _ = self._hua_plot_group_key(hua_for_scale)
			hua_counts = hua_for_scale.groupby("_hua_group_key", dropna=False)[self.hua_unit_col].nunique()
			max_hua = float(hua_counts.max()) if len(hua_counts) else 1.0
		else:
			max_hua = 1.0

		self._folium_scale_maxima_cache = {
			"expected_units": max(1.0, max_expected),
			"addpt_count": max(1.0, max_addpt),
			"hua_allocated_units": max(1.0, max_hua),
		}
		return self._folium_scale_maxima_cache

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

		nsi_3857, addpt_3857 = self._get_metric_layers()
		nsi_in_buffer = nsi_3857[nsi_3857.geometry.intersects(block_buffer_union)].copy(deep=True)
		nsi_in_block = nsi_3857[nsi_3857.geometry.intersects(block_polygon_union)].copy(deep=True)

		addpt_in_buffer = None
		addpt_in_block = None
		if addpt_3857 is not None:
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
			_addpt_agg = {
				"addpt_count": (self.bldg_uniqueid, "size"),
				"addpt_occtype": (self.addpt_occtype_col, lambda s: "|".join(sorted({str(v) for v in s.dropna()}))),
				"addpt_geometry": ("geometry", "first"),
			}
			if "residential" in addpt_block.columns:
				_addpt_agg["residential"] = ("residential", "first")
			addpt_counts = (
				addpt_block.groupby(self.bldg_uniqueid, dropna=False)
				.agg(**_addpt_agg)
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
			if "numprec" in hua_block.columns:
				numprec_sum = (
					hua_block.groupby(self.bldg_uniqueid, dropna=False)["numprec"]
					.sum()
					.reset_index(name="numprec_sum")
				)
				hua_counts = hua_counts.merge(numprec_sum, on=self.bldg_uniqueid, how="left")
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
		if "numprec_sum" in compare_gdf.columns:
			compare_gdf["numprec_sum"] = compare_gdf["numprec_sum"].fillna(0)
		else:
			compare_gdf["numprec_sum"] = 0

		compare_gdf["expected_minus_addpt"] = compare_gdf["expected_units"] - compare_gdf["addpt_count"]
		compare_gdf["addpt_minus_hua"] = compare_gdf["addpt_count"] - compare_gdf["hua_allocated_units"]
		compare_gdf["expected_minus_hua"] = (
			compare_gdf["expected_units"] - compare_gdf["hua_allocated_units"]
		)

		return compare_gdf

	def _compare_expected_to_addpt_by_occtype_for_block(self, block_id):
		"""Compare expected units to ADDPT counts by occupancy type for one block."""
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
			nsi_counts = (
				nsi_block.groupby(self.bldg_archetype_col, dropna=False)
				.size()
				.reset_index(name="nsi_count")
			)
		else:
			expected_by_occtype = pd.DataFrame(
				columns=[self.bldg_archetype_col, "expected_units"]
			)
			nsi_counts = pd.DataFrame(columns=[self.bldg_archetype_col, "nsi_count"])

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

		compare = compare.merge(nsi_counts, on=self.bldg_archetype_col, how="left")
		compare["expected_units"] = compare["expected_units"].fillna(0)
		compare["addpt_count"] = compare["addpt_count"].fillna(0)
		compare["nsi_count"] = compare["nsi_count"].fillna(0)
		compare["expected_minus_addpt"] = compare["expected_units"] - compare["addpt_count"]

		# Per-building rates help compare expected vs. observed points across occupancy types.
		denom = compare["nsi_count"].replace({0: pd.NA})
		compare["expected_units_per_nsi_building"] = compare["expected_units"] / denom
		compare["addpt_per_nsi_building"] = compare["addpt_count"] / denom
		compare[self.block_col] = self._normalize_blockid_series(pd.Series([block_id])).iloc[0]

		return compare.sort_values("expected_minus_addpt", ascending=False)

	def compare_expected_to_addpt_by_occtype(self, block_id=None):
		"""Compare expected units from archetypes to ADDPT counts by occupancy type.

		If block_id is provided, returns one block summary.
		If block_id is None, returns summaries for all blocks in HUA.
		"""
		if block_id is not None:
			return self._compare_expected_to_addpt_by_occtype_for_block(block_id)

		hua = self.hua_gdf.copy(deep=True)
		hua["_blockid"] = self._normalize_blockid_series(hua[self.block_col])
		block_ids = sorted(hua["_blockid"].dropna().unique())

		results = []
		for bid in block_ids:
			block_compare = self._compare_expected_to_addpt_by_occtype_for_block(bid)
			if not block_compare.empty:
				results.append(block_compare)

		if not results:
			return pd.DataFrame(
				columns=[
					self.block_col,
					self.bldg_archetype_col,
					"expected_units",
					"addpt_count",
					"nsi_count",
					"expected_minus_addpt",
					"expected_units_per_nsi_building",
					"addpt_per_nsi_building",
				]
			)

		all_blocks_compare = pd.concat(results, ignore_index=True)
		return all_blocks_compare.sort_values(
			by=[self.block_col, "expected_minus_addpt"],
			ascending=[True, False],
		)

	def compare_expected_to_addpt_by_occtype_for_blocks(self, block_ids):
		"""Run occtype comparison for a selected list of blocks only."""
		if block_ids is None:
			return pd.DataFrame()
		results = []
		for bid in block_ids:
			block_compare = self._compare_expected_to_addpt_by_occtype_for_block(bid)
			if not block_compare.empty:
				results.append(block_compare)
		if not results:
			return pd.DataFrame(
				columns=[
					self.block_col,
					self.bldg_archetype_col,
					"expected_units",
					"addpt_count",
					"nsi_count",
					"expected_minus_addpt",
					"expected_units_per_nsi_building",
					"addpt_per_nsi_building",
				]
			)
		combined = pd.concat(results, ignore_index=True)
		return combined.sort_values(by=[self.block_col, "expected_minus_addpt"], ascending=[True, False])

	def scan_residential_block_success(self):
		"""Fast block-level scan of expected-vs-ADDPT-vs-HUA agreement for residential buildings.

		This implementation avoids per-block spatial loops and instead joins structure-level
		totals to block IDs from HUA/ADDPT.
		"""
		lookup = self._archetype_expected_units_lookup()
		res_keys = {str(k) for k in lookup.keys() if str(k).upper().startswith("RES")}
		if not res_keys:
			warnings.warn("No residential archetypes found in lookup.")

		hua = self.hua_gdf.copy(deep=True)
		hua["_blockid"] = self._normalize_blockid_series(hua[self.block_col])
		hua_struct = (
			hua.groupby(self.bldg_uniqueid, dropna=False)
			.agg(
				blockid_hua=("_blockid", "first"),
				hua_residential_units=(self.hua_unit_col, "nunique"),
			)
			.reset_index()
		)

		nsi = self.nsi_gdf.copy(deep=True)
		nsi["_archetype_str"] = nsi[self.bldg_archetype_col].astype(str)
		nsi["expected_units"] = nsi["_archetype_str"].map(lookup).fillna(self.default_expected_units)
		nsi_res = nsi[nsi["_archetype_str"].isin(res_keys)].copy(deep=True)
		nsi_struct = (
			nsi_res.groupby(self.bldg_uniqueid, dropna=False)
			.agg(
				expected_residential_units=("expected_units", "sum"),
				nsi_residential_records=(self.bldg_uniqueid, "size"),
			)
			.reset_index()
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

			addpt_struct = (
				addpt.groupby(self.bldg_uniqueid, dropna=False)
				.agg(
					addpt_residential_points=(self.bldg_uniqueid, "size"),
					blockid_addpt=("_blockid", "first"),
				)
				.reset_index()
			)
		else:
			addpt_struct = pd.DataFrame(
				columns=[self.bldg_uniqueid, "addpt_residential_points", "blockid_addpt"]
			)

		merged = nsi_struct.merge(addpt_struct, on=self.bldg_uniqueid, how="outer")
		merged = merged.merge(hua_struct, on=self.bldg_uniqueid, how="outer")
		merged[self.block_col] = merged["blockid_hua"].where(
			merged["blockid_hua"].notna(), merged.get("blockid_addpt")
		)
		merged = merged[merged[self.block_col].notna()].copy(deep=True)

		if merged.empty:
			return pd.DataFrame(
				columns=[
					self.block_col,
					"nsi_residential_structures",
					"expected_residential_units",
					"addpt_residential_points",
					"hua_residential_units",
					"expected_minus_addpt",
					"addpt_minus_hua",
					"expected_minus_hua",
					"agreement_abs_total",
				]
			)

		for col in [
			"expected_residential_units",
			"addpt_residential_points",
			"hua_residential_units",
			"nsi_residential_records",
		]:
			if col in merged.columns:
				merged[col] = merged[col].fillna(0)

		summary = (
			merged.groupby(self.block_col, dropna=False)
			.agg(
				nsi_residential_structures=(self.bldg_uniqueid, "nunique"),
				expected_residential_units=("expected_residential_units", "sum"),
				addpt_residential_points=("addpt_residential_points", "sum"),
				hua_residential_units=("hua_residential_units", "sum"),
			)
			.reset_index()
		)

		summary["expected_minus_addpt"] = (
			summary["expected_residential_units"] - summary["addpt_residential_points"]
		)
		summary["addpt_minus_hua"] = (
			summary["addpt_residential_points"] - summary["hua_residential_units"]
		)
		summary["expected_minus_hua"] = (
			summary["expected_residential_units"] - summary["hua_residential_units"]
		)
		summary["agreement_abs_total"] = (
			summary["expected_minus_addpt"].abs()
			+ summary["addpt_minus_hua"].abs()
			+ summary["expected_minus_hua"].abs()
		)

		return summary.sort_values(
			by=["agreement_abs_total", "nsi_residential_structures"],
			ascending=[True, False],
		)

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

	# ------------------------------------------------------------------
	# Interactive Folium map (3 toggleable layers)
	# ------------------------------------------------------------------

	def prepare_folium_layer_data(self, block_id):
		"""Prepare cleaned NSI/ADDPT/HUA layers for a folium block map."""
		context = self.prepare_block_context(block_id)
		compare_gdf = self.build_expected_units_from_archetypes(block_id)

		wgs = f"epsg:{self.wgs84_epsg}"
		block_poly_4326 = context["block_polygon_3857"].to_crs(wgs)
		block_buf_4326 = context["block_buffer_3857"].to_crs(wgs)

		# Layer 1 (NSI): raw NSI records in block with archetype expected_units added
		nsi_raw = context["nsi_in_block"].copy(deep=True)
		if not nsi_raw.empty:
			_lookup = self._archetype_expected_units_lookup()
			nsi_raw["expected_units"] = (
				nsi_raw[self.bldg_archetype_col].astype(str).map(_lookup).fillna(self.default_expected_units)
			)
			nsi_layer = nsi_raw.to_crs(wgs)
		else:
			nsi_layer = nsi_raw

		# Layer 2 (ADDPT): raw ADDPT records in block grouped to structure level
		addpt_raw = context["addpt_in_block"]
		if addpt_raw is not None and not addpt_raw.empty:
			_addpt_agg = {
				"addpt_count": (self.bldg_uniqueid, "size"),
				"occtype": (self.addpt_occtype_col, lambda s: "|".join(sorted({str(v) for v in s.dropna()}))),
				"geometry": ("geometry", "first"),
			}
			if "residential" in addpt_raw.columns:
				_addpt_agg["residential"] = ("residential", "first")
			_addpt_struct = (
				addpt_raw.groupby(self.bldg_uniqueid, dropna=False)
				.agg(**_addpt_agg)
				.reset_index()
			)
			addpt_layer = gpd.GeoDataFrame(_addpt_struct, geometry="geometry", crs=addpt_raw.crs).to_crs(wgs)
		else:
			addpt_layer = gpd.GeoDataFrame(columns=[self.bldg_uniqueid, "addpt_count", "occtype", "geometry"])

		# Layer 3 (HUA): raw HUA records in block grouped for plotting.
		# Missing-building records are kept as distinct HUA-level points.
		hua_raw = context["block_hua_gdf"].copy(deep=True)
		if not hua_raw.empty:
			hua_raw_m = hua_raw.to_crs(epsg=self.metric_epsg)
			hua_raw_m["_hua_group_key"], missing_mask = self._hua_plot_group_key(hua_raw_m)
			hua_raw_m["missing_building_data"] = missing_mask.astype(int)
			_hua_agg = {
				"hua_allocated_units": (self.hua_unit_col, "nunique"),
				self.bldg_uniqueid: (self.bldg_uniqueid, "first"),
				"missing_building_data": ("missing_building_data", "max"),
				"geometry": ("geometry", "first"),
			}
			if "numprec" in hua_raw_m.columns:
				_hua_agg["numprec_sum"] = ("numprec", "sum")
			_hua_struct = (
				hua_raw_m.groupby("_hua_group_key", dropna=False)
				.agg(**_hua_agg)
				.reset_index()
			)
			hua_layer = gpd.GeoDataFrame(_hua_struct, geometry="geometry", crs=f"epsg:{self.metric_epsg}").to_crs(wgs)
		else:
			hua_layer = gpd.GeoDataFrame(columns=[self.bldg_uniqueid, "hua_allocated_units", "geometry"])

		return {
			"block_id": block_id,
			"context": context,
			"compare_gdf": compare_gdf,
			"block_poly_4326": block_poly_4326,
			"block_buf_4326": block_buf_4326,
			"nsi_layer": nsi_layer,
			"addpt_layer": addpt_layer,
			"hua_layer": hua_layer,
		}

	def plot_three_layer_folium_from_clean_layers(self, cleaned_layers, tiles="CartoDB positron", zoom_start=17):
		"""Render folium map using pre-cleaned layer datasets."""
		block_id = cleaned_layers["block_id"]
		compare_gdf = cleaned_layers["compare_gdf"]
		block_poly_4326 = cleaned_layers["block_poly_4326"]
		block_buf_4326 = cleaned_layers["block_buf_4326"]
		nsi_layer = cleaned_layers["nsi_layer"]
		addpt_layer = cleaned_layers["addpt_layer"]
		hua_layer = cleaned_layers["hua_layer"]

		# Map centre: centroid of the block polygon
		centroid = block_poly_4326.union_all().centroid
		m = folium.Map(location=[centroid.y, centroid.x], zoom_start=zoom_start, tiles=tiles)

		# Additional basemap: satellite imagery.
		folium.TileLayer(
			tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
			attr="Tiles © Esri",
			name="Satellite",
			overlay=False,
			control=True,
		).add_to(m)

		# ---- Title + summary table overlay ----
		# Build occtype counts from the compare_gdf (structure level)
		if not compare_gdf.empty and "occtype" in compare_gdf.columns:
			occtype_counts = (
				compare_gdf.groupby("occtype", dropna=False)
				.agg(
					nsi_structures=(self.bldg_uniqueid, "count"),
					expected_units=("expected_units", "sum"),
					addpt_count=("addpt_count", "sum"),
					hua_units=("hua_allocated_units", "sum"),
				)
				.reset_index()
				.sort_values("occtype", ascending=True)
			)
			total_pop = int(compare_gdf["numprec_sum"].sum()) if "numprec_sum" in compare_gdf.columns else 0
			table_rows = "".join(
				f"<tr><td>{r['occtype']}</td><td style='text-align:right'>{int(r['nsi_structures'])}</td>"
				f"<td style='text-align:right'>{int(r['expected_units'])}</td>"
				f"<td style='text-align:right'>{int(r['addpt_count'])}</td>"
				f"<td style='text-align:right'>{int(r['hua_units'])}</td></tr>"
				for _, r in occtype_counts.iterrows()
			)
			table_html = (
				"<table style='border-collapse:collapse;font-size:11px'>"
				"<thead><tr>"
				"<th style='border-bottom:1px solid #999;padding:2px 6px'>occtype</th>"
				"<th style='border-bottom:1px solid #999;padding:2px 6px'>structures</th>"
				"<th style='border-bottom:1px solid #999;padding:2px 6px'>exp units</th>"
				"<th style='border-bottom:1px solid #999;padding:2px 6px'>addpt</th>"
				"<th style='border-bottom:1px solid #999;padding:2px 6px'>HUA</th>"
				"</tr></thead>"
				f"<tbody>{table_rows}</tbody>"
				f"<tfoot><tr><td colspan='4' style='padding-top:4px'><b>Total population (numprec):</b></td>"
				f"<td style='text-align:right'><b>{total_pop}</b></td></tr></tfoot>"
				"</table>"
			)
		else:
			table_html = ""
		title_html = (
			f"<div style='position:fixed;top:10px;left:60px;z-index:9999;"
			f"background:white;padding:8px 12px;border:1px solid #aaa;"
			f"border-radius:4px;font-family:sans-serif;max-width:420px;box-shadow:2px 2px 4px rgba(0,0,0,0.2)'>"
			f"<b style='font-size:13px'>Validation of {block_id}</b><br><br>"
			f"{table_html}"
			f"</div>"
		)
		m.get_root().html.add_child(folium.Element(title_html))

		# Block boundary overlay
		folium.GeoJson(
			block_poly_4326.__geo_interface__,
			name="Block boundary",
			style_function=lambda _: {"color": "black", "weight": 2, "fillOpacity": 0.05},
		).add_to(m)

		# Buffer overlay
		folium.GeoJson(
			block_buf_4326.__geo_interface__,
			name="Block buffer",
			style_function=lambda _: {"color": "grey", "weight": 1, "fillOpacity": 0.0, "dashArray": "4 4"},
		).add_to(m)

		if nsi_layer.empty and addpt_layer.empty and hua_layer.empty:
			folium.LayerControl(collapsed=False).add_to(m)
			return m

		# Helper: one shared radius scale across layers so map-to-map comparisons are consistent.
		def _radius(value, scale_max, min_r=5, max_r=20):
			if not scale_max or scale_max <= 0:
				return min_r
			frac = max(0.0, min(1.0, float(value) / float(scale_max)))
			return min_r + (max_r - min_r) * frac

		# Use NSI expected-units as the common scale reference for all three layers.
		scale_maxima = self._get_folium_scale_maxima()
		common_scale_max = scale_maxima["expected_units"]

		# ---- Layer 1: NSI – Expected Units (from original NSI file) ----
		fg_nsi = folium.FeatureGroup(name="1. NSI / Expected Units", show=True)
		for _, row in nsi_layer.iterrows():
			if row.geometry is None:
				continue
			lat, lon = row.geometry.y, row.geometry.x
			fd_id = row.get(self.bldg_uniqueid, "")
			occtype = row.get(self.bldg_archetype_col, "")
			exp_units = int(row.get("expected_units", 0))
			popup_html = (
				f"<b>fd_id_bid:</b> {fd_id}<br>"
				f"<b>occtype:</b> {occtype}<br>"
				f"<b>expected_units:</b> {exp_units}"
			)
			folium.CircleMarker(
				location=[lat, lon],
				radius=_radius(exp_units, common_scale_max),
				color="steelblue",
				fill=True,
				fill_color="steelblue",
				fill_opacity=0.7,
				popup=folium.Popup(popup_html, max_width=280),
				tooltip=f"{occtype} | exp: {exp_units}",
			).add_to(fg_nsi)
		fg_nsi.add_to(m)

		# ---- Layer 2: ADDPT (from original ADDPT file, grouped to structure level) ----
		fg_addpt = folium.FeatureGroup(name="2. ADDPT (Points per Structure)", show=True)
		for _, row in addpt_layer.iterrows():
			if row.geometry is None:
				continue
			lat, lon = row.geometry.y, row.geometry.x
			fd_id = row.get(self.bldg_uniqueid, "")
			occtype = row.get("occtype", "")
			addpt_count = int(row.get("addpt_count", 0))
			residential = row.get("residential", "")
			is_residential = bool(residential) and str(residential) not in ("0", "False", "false", "")
			popup_html = (
				f"<b>fd_id_bid:</b> {fd_id}<br>"
				f"<b>occtype:</b> {occtype}<br>"
				f"<b>residential:</b> {residential}<br>"
				f"<b>addpt_count:</b> {addpt_count}"
			)
			tooltip_str = f"{occtype} | res: {residential} | addpt: {addpt_count}"
			if is_residential:
				folium.CircleMarker(
					location=[lat, lon],
					radius=_radius(addpt_count, common_scale_max),
					color="darkorange",
					fill=True,
					fill_color="darkorange",
					fill_opacity=0.7,
					popup=folium.Popup(popup_html, max_width=280),
					tooltip=tooltip_str,
				).add_to(fg_addpt)
			else:
				# Non-residential: render as a square using DivIcon
				px = max(8, int(_radius(addpt_count, common_scale_max) * 2))
				folium.Marker(
					location=[lat, lon],
					icon=folium.DivIcon(
						html=(
							f'<div style="width:{px}px;height:{px}px;'
							f'background:darkorange;opacity:0.7;'
							f'border:1px solid #b8600a;"></div>'
						),
						icon_size=(px, px),
						icon_anchor=(px // 2, px // 2),
					),
					popup=folium.Popup(popup_html, max_width=280),
					tooltip=tooltip_str,
				).add_to(fg_addpt)
		fg_addpt.add_to(m)

		# ---- Layer 3: HUA (from original HUA file, grouped to structure level) ----
		fg_hua = folium.FeatureGroup(name="3. HUA (Allocated Units)", show=True)
		for _, row in hua_layer.iterrows():
			if row.geometry is None:
				continue
			hua_units = int(row.get("hua_allocated_units", 0))
			if hua_units == 0:
				continue
			lat, lon = row.geometry.y, row.geometry.x
			fd_id = row.get(self.bldg_uniqueid, "")
			missing_flag = int(row.get("missing_building_data", 0))
			numprec_sum = int(row.get("numprec_sum", 0))
			missing_line = f"<b>missing_building_data:</b> {missing_flag}<br>" if missing_flag else ""
			popup_html = (
				f"<b>fd_id_bid:</b> {fd_id}<br>"
				f"{missing_line}"
				f"<b>hua_allocated_units:</b> {hua_units}<br>"
				f"<b>numprec_sum:</b> {numprec_sum}"
			)
			folium.CircleMarker(
				location=[lat, lon],
				radius=_radius(hua_units, common_scale_max),
				color="crimson",
				fill=True,
				fill_color="crimson",
				fill_opacity=0.7,
				popup=folium.Popup(popup_html, max_width=280),
				tooltip=f"fd_id: {fd_id} | missing: {missing_flag} | hua: {hua_units} | numprec: {numprec_sum}",
			).add_to(fg_hua)
		fg_hua.add_to(m)

		folium.LayerControl(collapsed=False).add_to(m)
		return m

	def plot_three_layer_folium(self, block_id, tiles="CartoDB positron", zoom_start=17):
		"""Return an interactive folium map for *block_id* with three toggleable layers."""
		cleaned_layers = self.prepare_folium_layer_data(block_id)
		return self.plot_three_layer_folium_from_clean_layers(
			cleaned_layers,
			tiles=tiles,
			zoom_start=zoom_start,
		)

