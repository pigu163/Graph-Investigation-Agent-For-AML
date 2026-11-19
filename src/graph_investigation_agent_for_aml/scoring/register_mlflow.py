"""Register a trained local model and its evaluation in MLflow."""
import argparse
import json
from pathlib import Path

import mlflow


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--name", default="aml-xgboost")
    args = parser.parse_args()
    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment("AML HI-Small")
    metadata = json.loads((args.model_dir / "metrics.json").read_text())
    with mlflow.start_run() as run:
        mlflow.log_metrics({key: value for key, value in metadata.items() if key.startswith("test_")})
        mlflow.log_artifact(str(args.model_dir / "model.json"))
        mlflow.log_artifact(str(args.model_dir / "metrics.json"))
        client = mlflow.MlflowClient()
        try:
            client.get_registered_model(args.name)
        except mlflow.exceptions.MlflowException as exc:
            if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise
            client.create_registered_model(args.name)
        version = client.create_model_version(
            args.name, run.info.artifact_uri + "/model.json", run.info.run_id
        )
        print(f"Registered {args.name} version {version.version}, run {run.info.run_id}")


if __name__ == "__main__":
    main()
