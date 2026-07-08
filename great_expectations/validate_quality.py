import argparse
import hashlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
import s3fs


DEFAULT_INPUT_PATH = "curated/stations_enrichies"
DEFAULT_QUARANTINE_PATH = "quarantine/data-quality"
DEFAULT_ACCEPTED_PATH = "curated-quality/accepted"
DEFAULT_REPORT_PATH = "quality-reports/data-quality/report.json"

REQUIRED_COLUMNS = [
    "stationcode",
    "taux_occupation",
    "capacity",
    "numbikesavailable",
    "meteo_temp",
]


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_s3_path(path: str) -> str:
    return path.removeprefix("s3://").strip("/")


def join_s3(base: str, *parts: str) -> str:
    cleaned = [normalize_s3_path(base)] + [str(p).strip("/") for p in parts]
    return "/".join(part for part in cleaned if part)


def ensure_bucket(fs: s3fs.S3FileSystem, path: str) -> str:
    """Create the S3/MinIO bucket used by *path* when it does not exist."""
    normalized = normalize_s3_path(path)
    bucket = normalized.split("/", 1)[0]
    if not bucket:
        raise ValueError(f"Chemin S3 invalide : {path}")
    if not fs.exists(bucket):
        fs.mkdir(bucket)
        print(f"PASS - bucket MinIO cree : {bucket}")
    else:
        print(f"PASS - bucket MinIO disponible : {bucket}")
    return bucket


def dataframe_sha256(df: pd.DataFrame) -> str:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return hashlib.sha256(buffer.getvalue()).hexdigest()


def write_parquet(fs: s3fs.S3FileSystem, df: pd.DataFrame, path: str) -> None:
    normalized = normalize_s3_path(path)
    with fs.open(normalized, "wb") as handle:
        df.to_parquet(handle, index=False)


def write_json(fs: s3fs.S3FileSystem, payload: dict[str, Any], path: str) -> None:
    normalized = normalize_s3_path(path)
    parent = normalized.rsplit("/", 1)[0]
    if parent:
        fs.makedirs(parent, exist_ok=True)
    with fs.open(normalized, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)


def list_input_files(fs: s3fs.S3FileSystem, input_path: str) -> list[str]:
    normalized = normalize_s3_path(input_path)
    try:
        files = fs.find(normalized)
    except FileNotFoundError:
        return []
    return sorted(
        file_path
        for file_path in files
        if file_path.lower().endswith((".parquet", ".csv"))
    )


def read_input(fs: s3fs.S3FileSystem, input_path: str) -> tuple[pd.DataFrame, list[str]]:
    files = list_input_files(fs, input_path)
    if not files:
        raise FileNotFoundError(
            f"Aucun fichier Parquet ou CSV trouve sous s3://{normalize_s3_path(input_path)}"
        )

    frames: list[pd.DataFrame] = []
    for file_path in files:
        if file_path.lower().endswith(".parquet"):
            with fs.open(file_path, "rb") as handle:
                frames.append(pd.read_parquet(handle))
        else:
            with fs.open(file_path, "rb") as handle:
                frames.append(pd.read_csv(handle))

    return pd.concat(frames, ignore_index=True), files


def create_demo_dataset() -> pd.DataFrame:
    # 5 lignes valides et 3 lignes volontairement invalides.
    return pd.DataFrame(
        [
            {"stationcode": "1001", "taux_occupation": 20.0, "capacity": 30, "numbikesavailable": 6, "meteo_temp": 18.2},
            {"stationcode": "1002", "taux_occupation": 50.0, "capacity": 40, "numbikesavailable": 20, "meteo_temp": 19.1},
            {"stationcode": "1003", "taux_occupation": 80.0, "capacity": 25, "numbikesavailable": 20, "meteo_temp": 17.8},
            {"stationcode": "1004", "taux_occupation": 0.0, "capacity": 15, "numbikesavailable": 0, "meteo_temp": 21.0},
            {"stationcode": "1005", "taux_occupation": 100.0, "capacity": 12, "numbikesavailable": 12, "meteo_temp": 16.4},
            {"stationcode": None, "taux_occupation": 45.0, "capacity": 20, "numbikesavailable": 9, "meteo_temp": 20.0},
            {"stationcode": "BAD-2", "taux_occupation": 120.0, "capacity": 20, "numbikesavailable": 30, "meteo_temp": 18.0},
            {"stationcode": "BAD-3", "taux_occupation": 30.0, "capacity": -1, "numbikesavailable": 250, "meteo_temp": 80.0},
        ]
    )


def quality_errors(row: pd.Series) -> list[str]:
    errors: list[str] = []
    stationcode = row.get("stationcode")
    if pd.isna(stationcode) or not str(stationcode).strip():
        errors.append("stationcode_null_or_empty")

    def numeric(name: str) -> float | None:
        value = pd.to_numeric(pd.Series([row.get(name)]), errors="coerce").iloc[0]
        return None if pd.isna(value) else float(value)

    taux = numeric("taux_occupation")
    capacity = numeric("capacity")
    bikes = numeric("numbikesavailable")
    temperature = numeric("meteo_temp")

    if taux is None or not 0 <= taux <= 100:
        errors.append("taux_occupation_out_of_range")
    if capacity is None or not 0 <= capacity <= 200:
        errors.append("capacity_out_of_range")
    if bikes is None or not 0 <= bikes <= 200:
        errors.append("numbikesavailable_out_of_range")
    if temperature is None or not -30 <= temperature <= 50:
        errors.append("meteo_temp_out_of_range")
    if capacity is not None and bikes is not None and bikes > capacity:
        errors.append("numbikesavailable_greater_than_capacity")

    return errors


def expectation_to_dict(result: Any) -> dict[str, Any]:
    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()
    return dict(result)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validation et quarantaine des donnees VelibData")
    parser.add_argument("--input-path", default=os.getenv("QUALITY_INPUT_PATH", DEFAULT_INPUT_PATH))
    parser.add_argument("--quarantine-path", default=os.getenv("QUALITY_QUARANTINE_PATH", DEFAULT_QUARANTINE_PATH))
    parser.add_argument("--accepted-path", default=os.getenv("QUALITY_ACCEPTED_PATH", DEFAULT_ACCEPTED_PATH))
    parser.add_argument("--report-path", default=os.getenv("QUALITY_REPORT_PATH", DEFAULT_REPORT_PATH))
    parser.add_argument("--run-id", default=os.getenv("QUALITY_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--demo", action="store_true", default=env_bool("QUALITY_DEMO", False))
    parser.add_argument("--strict", action="store_true", default=env_bool("QUALITY_STRICT", False))
    args = parser.parse_args()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    user = os.getenv("MINIO_ROOT_USER")
    password = os.getenv("MINIO_ROOT_PASSWORD")
    local_report = os.getenv("QUALITY_LOCAL_REPORT_PATH", "").strip()

    print("=== Validation qualite et quarantaine — VelibData ===")
    print(f"RUN_ID={args.run_id}")

    if not user or not password:
        print("ERREUR : identifiants MinIO absents.")
        return 1

    fs = s3fs.S3FileSystem(
        key=user,
        secret=password,
        client_kwargs={"endpoint_url": endpoint},
    )

    input_path = normalize_s3_path(args.input_path)
    quarantine_dir = join_s3(args.quarantine_path, args.run_id)
    accepted_dir = join_s3(args.accepted_path, args.run_id)
    report_path = normalize_s3_path(args.report_path)

    try:
        # A fresh MinIO instance may not yet contain the dedicated quality
        # buckets. Create only the top-level buckets required by this run.
        for required_path in (input_path, quarantine_dir, accepted_dir, report_path):
            ensure_bucket(fs, required_path)

        if args.demo:
            demo_df = create_demo_dataset()
            demo_path = join_s3(input_path, "input.parquet")
            write_parquet(fs, demo_df, demo_path)
            print(f"PASS - jeu de demonstration ecrit : s3://{demo_path}")

        df, source_files = read_input(fs, input_path)
        print(f"PASS - {len(df)} lignes chargees depuis {len(source_files)} fichier(s).")

        missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
        if missing_columns:
            raise ValueError(f"Colonnes obligatoires manquantes : {', '.join(missing_columns)}")

        # Great Expectations produit une preuve de validation au niveau dataset.
        gxdf = gx.from_pandas(df.copy())
        expectation_results = [
            gxdf.expect_table_row_count_to_be_between(min_value=1, max_value=100000),
            gxdf.expect_column_values_to_not_be_null("stationcode"),
            gxdf.expect_column_values_to_be_between("taux_occupation", min_value=0, max_value=100),
            gxdf.expect_column_values_to_be_between("capacity", min_value=0, max_value=200),
            gxdf.expect_column_values_to_be_between("numbikesavailable", min_value=0, max_value=200),
            gxdf.expect_column_values_to_be_between("meteo_temp", min_value=-30, max_value=50),
        ]
        expectation_dicts = [expectation_to_dict(result) for result in expectation_results]

        errors_per_row = df.apply(quality_errors, axis=1)
        valid_mask = errors_per_row.map(len).eq(0)
        accepted_df = df.loc[valid_mask].copy()
        quarantine_df = df.loc[~valid_mask].copy()
        if not quarantine_df.empty:
            quarantine_df["_quality_errors"] = errors_per_row.loc[~valid_mask].map(lambda values: "|".join(values))

        accepted_path = join_s3(accepted_dir, "accepted.parquet")
        quarantine_path = join_s3(quarantine_dir, "invalid.parquet")

        if not accepted_df.empty:
            write_parquet(fs, accepted_df, accepted_path)
        if not quarantine_df.empty:
            write_parquet(fs, quarantine_df, quarantine_path)

        reason_counts: dict[str, int] = {}
        for row_errors in errors_per_row:
            for reason in row_errors:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

        passed_expectations = sum(1 for result in expectation_dicts if result.get("success"))
        total_expectations = len(expectation_dicts)
        overall_pass = quarantine_df.empty or not args.strict
        status = "PASS" if quarantine_df.empty else ("PASS_WITH_QUARANTINE" if overall_pass else "FAIL")

        report: dict[str, Any] = {
            "run_id": args.run_id,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "strict_mode": args.strict,
            "source": {
                "input_path": f"s3://{input_path}",
                "files": [f"s3://{item}" for item in source_files],
                "rows": int(len(df)),
                "sha256_parquet_canonical": dataframe_sha256(df),
            },
            "counts": {
                "input_rows": int(len(df)),
                "accepted_rows": int(len(accepted_df)),
                "quarantined_rows": int(len(quarantine_df)),
            },
            "outputs": {
                "accepted_path": f"s3://{accepted_path}" if not accepted_df.empty else None,
                "quarantine_path": f"s3://{quarantine_path}" if not quarantine_df.empty else None,
                "report_path": f"s3://{report_path}",
            },
            "invalid_reason_counts": reason_counts,
            "great_expectations": {
                "passed": passed_expectations,
                "total": total_expectations,
                "results": expectation_dicts,
            },
        }

        write_json(fs, report, report_path)
        if local_report:
            local_path = Path(local_report)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        # Verification technique des sorties apres ecriture.
        if not fs.exists(report_path):
            raise RuntimeError("Le rapport qualite n'est pas present dans MinIO.")
        if not accepted_df.empty and not fs.exists(accepted_path):
            raise RuntimeError("La sortie acceptee n'est pas presente dans MinIO.")
        if not quarantine_df.empty and not fs.exists(quarantine_path):
            raise RuntimeError("La quarantaine n'est pas presente dans MinIO.")

        print("\n=== Resultats Great Expectations ===")
        for result in expectation_dicts:
            config = result.get("expectation_config", {})
            exp_type = config.get("expectation_type", "expectation")
            column = config.get("kwargs", {}).get("column", "table")
            mark = "PASS" if result.get("success") else "FAIL"
            print(f"{mark} - {exp_type} ({column})")

        print("\n=== Routage qualite ===")
        print(f"INPUT_ROWS={len(df)}")
        print(f"ACCEPTED_ROWS={len(accepted_df)}")
        print(f"QUARANTINED_ROWS={len(quarantine_df)}")
        print(f"PASS - donnees acceptees : s3://{accepted_path}" if not accepted_df.empty else "INFO - aucune donnee acceptee")
        print(f"PASS - donnees invalides isolees : s3://{quarantine_path}" if not quarantine_df.empty else "PASS - aucune donnee invalide")
        print(f"PASS - rapport qualite : s3://{report_path}")
        if local_report:
            print(f"PASS - copie locale du rapport : {local_report}")

        if overall_pass:
            print("RESULTAT GLOBAL : PASS")
            return 0

        print("RESULTAT GLOBAL : FAIL")
        return 1

    except Exception as exc:  # noqa: BLE001 - le rapport console doit rester explicite
        print(f"ERREUR : {exc}")
        print("RESULTAT GLOBAL : FAIL")
        return 1


if __name__ == "__main__":
    sys.exit(main())
