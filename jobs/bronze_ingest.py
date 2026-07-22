"""Bronze ingestion — re-exports from jobs.bronze.offline."""

from jobs.bronze.offline import (
    CONTRACTS_DIR,
    add_ingest_metadata,
    get_spark,
    load_contract,
    main,
    read_landing_parquet,
    validate_contract,
)
