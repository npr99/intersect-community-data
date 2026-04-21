# pyncoda Researcher FAQ

This FAQ is intended **for researchers using pyncoda**, the Housing Unit Allocation (HUA) method, and related components of the intersect-community-data project. It synthesizes technical questions received over the years and provides structured, research-oriented responses. Each FAQ entry includes the researcher who originally asked the question, their affiliation, and the date.

---

## 1. Methodology & Statistical Foundations

### 1.1 Does the HUA method capture correlations between housing unit characteristics and household demographics?

**Asked by:** Himadri Sen Gupta, Colorado State University Pueblo (Feb 2026)

The HUA method preserves demographic distributions at the Census block level, including race, ethnicity, household size, vacancy, and tenure. However, it does **not** explicitly model joint (multivariate) correlations between these demographic properties and **building characteristics**, because the structural data (e.g., building type, number of stories, footprint attributes) come from external datasets such as NSI, OSM, or Microsoft Building Footprints.

**In summary:**

- ✔️ Correlations within the Census tables **are preserved** (e.g., race × tenure).
- ❌ Cross-dataset correlations (e.g., building type × ethnicity) **are not explicitly modeled**.

**Relevant code:**

- `pyncoda/hua/hua_assignment.py`
- `pyncoda/hua/hua_mcs_loop.ipynb`

---

### 1.2 Does the HUA method avoid ecological fallacy?

**Asked by:** Amina Meselhe, Oregon State University (Sept 2025)

Yes. Because HUA transforms Census block tabulations into **unit-level records**, it avoids the ecological fallacy inherent in using block-level averages. Each unit inherits attributes directly from the block-level distributions before being allocated to buildings.

**Relevant code:**

- `pyncoda/hua/hua_assignment.py`
- `pyncoda/hua/hua_codebook_generator.ipynb`

---

### 1.3 What assumptions underlie the matching of units to buildings?

**Asked by:** Amina Meselhe, Oregon State University (Sept 2025)

The primary assumption is that **within-block units can be randomly assigned** to buildings with minimal bias. Monte Carlo Simulation (MCS) checks this assumption by repeatedly allocating units and quantifying variation.

**Relevant code:**

- `pyncoda/hua/hua_mcs_loop.ipynb`

---

## 2. Monte Carlo Simulation (MCS)

### 2.1 How many Monte Carlo iterations should be run?

**Asked by:** Amina Meselhe, Oregon State University (Sept 2025)

There is no universal number. The goal is to quantify uncertainty:

- For small counties (<40k housing units), 10–50 iterations is typical.
- For publication-quality uncertainty quantification, researchers sometimes run 200–500+ iterations.

Note that the current HUA workflow is **not optimized**, so large MCS runs generate significant output.

**Relevant code:**

- `pyncoda/hua/hua_mcs_loop.ipynb`

---

## 3. Data Sources & Preparation

### 3.1 What building data should I use with pyncoda?

**Asked by:** Oscar Wecker, Oregon State University (May 2025)

You may use:

- OpenStreetMap (OSM) building footprints
- National Structure Inventory (NSI)
- Microsoft Building Footprints
- Data Axle (commercial structures)

However, **vacant structure identification** depends largely on Census block vacancy counts, not external datasets.

**Relevant code:**

- `pyncoda/CommunitySourceData/`

---

### 3.2 How do I integrate disability data?

**Asked by:** Abigail Beck & Swastika Barua, University of Houston (Feb 2026)

The disability extension requires adding new fields to the person-level files and updating the household characteristic integration logic.

A full implementation guide is available:

- `Readings/pyncoda_disability_implementation_guide.md`

---

## 4. Troubleshooting & Environment

### 4.1 Why am I receiving import errors (e.g., ModuleNotFoundError: fpdf)?

**Asked by:** Ram Krishna Mazumder, Case Western Reserve University (Jan 2023)

This typically occurs when the environment is missing dependencies listed in `environment.yml`. Installing `fpdf2` resolves the PDF-related import failure.

**Relevant code:**

- `environment.yml`

---

### 4.2 Why does my notebook fail to run due to missing input folders (e.g., OutputData)?

**Asked by:** Lisa Wang, Old Dominion University / CSU Affiliate (Mar 2025)

The pyncoda notebooks require a predefined folder structure. Missing folders indicate that the data download or pre-processing step was not completed.

**Relevant code:**

- `pyncoda/ncoda_06a_PDF_functions.py`
- Example input directory: `SampleOutputData/`

---

## 5. IN-CORE Integration

### 5.1 How do I connect socio-economic datasets to IN-CORE?

**Asked by:** Marsha Schoolcraft, University of Oklahoma (Jan 2025)

IN-CORE requires a specific schema for household and housing unit datasets. The codebook in `SampleOutputData/` explains the required structure.

**Relevant code:**

- `SampleOutputData/hua_*_codebook.pdf`

---

# 📚 Appendix: Chronological List of Researcher Questions

This appendix provides a time-ordered (oldest → newest) record of technical questions that informed the FAQ.

### Jan 2023 — Ram Krishna Mazumder (Case Western Reserve University)

- Asked about environment errors and missing dependencies.

### Jan 2025 — Marsha Schoolcraft (University of Oklahoma)

- Asked how to connect socio-economic datasets into IN-CORE and locate HUA codebooks.

### Mar 2025 — Lisa Wang (Old Dominion University / CSU Affiliate)

- Asked about missing input folders and running pyncoda notebooks.

### May 2025 — Oscar Wecker (Oregon State University)

- Asked about vacant structures and reaggregation workflow.

### Sept 2025 — Amina Meselhe (Oregon State University)

- Asked about ecological fallacy, MCS iteration counts, and random allocation assumptions.

### Feb 2026 — Himadri Sen Gupta (Colorado State University Pueblo)

- Asked about HUA correlations between building characteristics and household demographics.

### Feb 2026 — Abigail Beck & Swastika Barua (University of Houston)

- Asked about extending HUA to include disability data.

---

If you have suggestions for additional FAQ entries or encounter new research-related challenges, please contribute to the repository or contact the maintainer. 🚀
