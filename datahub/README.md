# DataHub lineage

The default pipeline stack stays small enough for a coursework laptop, so the
full DataHub quickstart is launched separately. It needs at least 8 GB of
Docker memory in addition to this project's services.

## Publish the lineage

```bash
python -m pip install 'acryl-datahub[datahub-lineage-file]'

# Spark already owns host port 8080, so remap DataHub GMS.
export DATAHUB_MAPPED_GMS_PORT=8084
datahub docker quickstart

datahub ingest -c datahub/recipe.yml
```

Open <http://localhost:9002> with `datahub` / `datahub`, then inspect:

- `vnstock.bronze.raw_ohlcv_daily` for DP1;
- `vnstock.gold.fact_daily_price` for DP2;
- `vnstock.gold.feat_ticker_unified` for DP3.

`lineage.yml` uses DataHub's version-1 file-based lineage format. It records
dataset-level DP1, DP2 and DP3 relationships only. JSON schemas remain
versioned in `contracts/`, but this recipe does not publish them, Airflow jobs,
validation assertions or contract entities to DataHub. Additional ingestion
must be implemented before claiming the rubric's validation/contract tabs;
do not use an empty tab as proof.

The GMS remap is local-shell configuration only. Do not commit credentials or
personal access tokens.
