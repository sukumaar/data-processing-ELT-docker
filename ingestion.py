#!/usr/bin/env python3
from pathlib import Path
from typing import Any
import subprocess
import sys
import runpy
from typing import Dict
from typing import List
import uuid

def ingest_process(id:str, command_args: List[str]) -> Any:
    print(f"Running pipeline ingestion-{id}")
    prefix:List[str]= ["uvx","ingestr", "ingest","--yes"]
    final_args:List[str] = prefix + command_args
    print(f"using this as args: {final_args}")
    process = subprocess.Popen(
        final_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    print(stdout.decode())

def tranformation_process(id:str,config: Dict[str, str]) -> Any:
    print(f"Running pipeline transformation-{id}")
    directory:str=config["transformation"]["directory"]
    final_args:List[str]= ["dbt", "run", "--project-dir", 
                       directory, "--profiles-dir", directory]
    print(f"using this as args: {final_args}")
    process = subprocess.Popen(
        final_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    print(stdout.decode())
    

def createParam(config: Dict[str, str]) -> List[str]:
    engine = config["engine"].lower()
    name = config["name"].lower()
    match engine:
        case "ingestr":
            print("calling ingestr")
            source_connector = config["source"]["connector"]
            destination_connector = config["destination"]["connector"]
            source_uri = ""
            match source_connector:
                case "csv":
                    source_table = config["source"]["source_table"]
                    source_path = config["source"]["source_path"]
                    source_uri = f"csv://{source_path}"
                case _:
                    raise TypeError(f"Unknown source {source_connector}")
            match destination_connector:
                case "duckdb":
                    desitnation_path = config["destination"][
                        "destination_path"
                    ]
                    destination_table = config["destination"][
                        "destination_table"
                    ]
                    destination_uri = f"duckdb:///{desitnation_path}"
                case _:
                    raise TypeError(f"Unknown destination {destination_connector}")
            command_args = [
                "--source-uri",
                source_uri,
                "--source-table",
                source_table,
                "--dest-table",
                destination_table,
                "--dest-uri",
                destination_uri,
            ]
            return command_args
        case _:
            print(f"Unknown engine: {engine}")
    raise TypeError(f"something goes wrong")


def load_config(config_path: str) -> Any:
    """Load a Python config file and return its variables as a dict."""
    all_vars = runpy.run_path(config_path)
    config_vars = {
        k: v for k, v in all_vars.items() if not k.startswith("__") and not callable(v)
    }
    return config_vars


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ingestion.py <config_file.py>")
        sys.exit(1)

    config_file = sys.argv[1]
    config = load_config(config_file)
    id={config["name"]}-{uuid.uuid4()}
    ingest_process(id, createParam(config))
    tranformation_process(id,config)
