# CAO-DF system design diagrams

UML and ER diagrams of the CAO-DF framework and study code. They are drawn from the code
in `src/` and the CSV schemas in `results/`, so they show what was actually built. They are
supporting material (thesis, viva, repository readers) and are **not** part of the 8-page paper.

| # | Diagram | What it shows |
|---|---|---|
| 01 | Use case | Actors (researcher, data engineer, Spark) and what each does with CAO-DF |
| 02 | Component | Python modules, their dependencies and the data files they read and write |
| 03 | Class | Main classes, dataclasses and module functions with real signatures |
| 04 | ERD | Generated datasets and result tables (`workload_index`, `experiment_results`, `cost_per_cell`, `breakeven_table`, validation tables) |
| 05 | Sequence: `--decide` | How a recommendation is produced (profiles, nearest-workload estimate, total-cost rule) |
| 06 | Sequence: measured run | Randomized-block protocol, warm-up, sparkMeasure and psutil capture, CSV append |
| 07 | Activity: decision rule | Noise floor δ, TC = r + N(1 − s) per action, lowest TC wins, ties go to Nothing |
| 08 | Activity: study workflow | Setup → data → E1–E6 → analysis → paper, one script per step, in run order |
| 09 | Deployment | Study laptop, Python venv, JVM / Spark local[*], file system, price constant |

Experiment IDs: E1 shuffle gate, E2 partition sweep, E3 formal A1, E4 A2 pilot, E5 formal A2,
E6 validation. Only E3, E5 and E6 feed the analysis.

## Colour key

| Colour | Meaning |
|---|---|
| Blue | Infrastructure and decision core |
| Green | Workloads and generated data |
| Orange | Experiments and the engineer-facing entry point |
| Purple | Analysis and cost model outputs |
| Yellow | Data stores (CSV, Parquet) |
| Teal | External systems (Spark, sparkMeasure) |

The shared theme is in `src/_style.iuml`.

## Folders

- `src/`: PlantUML sources (edit these)
- `png/`: raster images for slides and documents
- `svg/`: vector images for the web
- `pdf/`: vector, one page each, for LaTeX or printing

## Regenerating

You need Java 17 and the PlantUML jar (tested with 1.2024.7). Graphviz is not required: the
non-sequence diagrams use the built-in `smetana` layout (`!pragma layout smetana`).

```powershell
cd paper\system_design
java -jar plantuml.jar -charset UTF-8 -tpng -o ..\png "src\0*.puml"
java -jar plantuml.jar -charset UTF-8 -tsvg -o ..\svg "src\0*.puml"
```

The PDFs were made by printing each SVG to a page of the same size with headless Microsoft Edge
(`--print-to-pdf`). This keeps the vectors and the fonts as they appear in the SVG.
