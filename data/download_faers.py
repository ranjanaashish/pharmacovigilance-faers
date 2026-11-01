"""Bulk download script for FDA Adverse Event Reporting System (FAERS) data.

Fetches the OpenFDA download manifest from https://api.fda.gov/download.json,
filters drug adverse event partitions based on year range and file limits,
downloads the partition ZIP files, and extracts JSON datasets into data/raw/.
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

# Set up project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"
OPENFDA_MANIFEST_URL = "https://api.fda.gov/download.json"


def fetch_manifest(manifest_url: str = OPENFDA_MANIFEST_URL, max_retries: int = 3, timeout: int = 30) -> Dict[str, Any]:
    """Fetch and parse the OpenFDA download manifest JSON.

    Parameters
    ----------
    manifest_url : str
        URL of the OpenFDA download manifest.
    max_retries : int, default=3
        Number of retry attempts for network errors.
    timeout : int, default=30
        HTTP request timeout in seconds.

    Returns
    -------
    dict
        Parsed JSON dictionary of the OpenFDA manifest.

    Raises
    ------
    RuntimeError
        If manifest cannot be retrieved after max_retries.
    """
    headers = {
        "User-Agent": "Pharmacovigilance-FAERS-ETL/1.0 (Academic Research; Big Data Analytics ED547)"
    }
    req = urllib.request.Request(manifest_url, headers=headers)

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[INFO] Fetching OpenFDA manifest from {manifest_url} (Attempt {attempt}/{max_retries})...")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                if response.status == 200:
                    data = response.read().decode("utf-8")
                    manifest = json.loads(data)
                    print(f"[INFO] Successfully fetched and parsed OpenFDA manifest.")
                    return manifest
                else:
                    print(f"[WARNING] Non-200 status code received: {response.status}")
        except urllib.error.HTTPError as e:
            print(f"[ERROR] HTTP Error {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            print(f"[ERROR] URL / Network Error: {e.reason}")
        except TimeoutError:
            print(f"[ERROR] Request timed out after {timeout} seconds.")
        except Exception as e:
            print(f"[ERROR] Unexpected error while fetching manifest: {e}")

        if attempt < max_retries:
            sleep_time = attempt * 2
            print(f"[INFO] Waiting {sleep_time} seconds before retrying...")
            time.sleep(sleep_time)

    raise RuntimeError(f"Failed to fetch OpenFDA manifest from {manifest_url} after {max_retries} attempts.")


def parse_drug_event_partitions(
    manifest: Dict[str, Any],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    max_files: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Extract and filter drug/event partitions from the OpenFDA manifest.

    Parameters
    ----------
    manifest : dict
        OpenFDA manifest dictionary.
    start_year : int, optional
        Earliest year to include (inclusive).
    end_year : int, optional
        Latest year to include (inclusive).
    max_files : int, optional
        Maximum number of partition files to download.

    Returns
    -------
    list of dict
        Filtered partition dictionaries containing 'file', 'size_mb', 'display_name', etc.
    """
    try:
        drug_event_data = manifest["results"]["drug"]["event"]
        partitions: List[Dict[str, Any]] = drug_event_data.get("partitions", [])
    except KeyError as exc:
        raise ValueError("Invalid manifest structure: could not locate 'results.drug.event.partitions'.") from exc

    print(f"[INFO] Found {len(partitions)} total drug/event partitions in manifest.")

    filtered_partitions: List[Dict[str, Any]] = []

    for partition in partitions:
        file_url = partition.get("file", "")
        display_name = partition.get("display_name", "")

        # Extract 4-digit year from partition URL or display name
        # Typical format: .../drug/event/2023q1/drug-event-0001-of-0030.json.zip
        year_match = re.search(r"/(19\d\d|20\d\d)q[1-4]/", file_url)
        if not year_match:
            year_match = re.search(r"\b(19\d\d|20\d\d)\b", display_name)

        partition_year: Optional[int] = int(year_match.group(1)) if year_match else None

        # Filter by start_year and end_year if year was detected
        if partition_year is not None:
            if start_year is not None and partition_year < start_year:
                continue
            if end_year is not None and partition_year > end_year:
                continue
        elif start_year is not None or end_year is not None:
            # If year filtering requested but year not found, skip partition
            continue

        filtered_partitions.append(partition)

        if max_files is not None and len(filtered_partitions) >= max_files:
            print(f"[INFO] Reached requested limit of {max_files} partition files.")
            break

    print(
        f"[INFO] Selected {len(filtered_partitions)} partitions matching criteria "
        f"(start_year={start_year}, end_year={end_year}, max_files={max_files})."
    )
    return filtered_partitions


def download_file_with_progress(
    url: str,
    destination_path: Path,
    expected_size_mb: Optional[float] = None,
    chunk_size: int = 1024 * 1024,  # 1 MB chunk
    max_retries: int = 3,
) -> bool:
    """Download a file via HTTP with a console progress bar and retry logic.

    Parameters
    ----------
    url : str
        Source URL.
    destination_path : Path
        Local target path for the downloaded file.
    expected_size_mb : float, optional
        Reported size in MB for progress estimation.
    chunk_size : int, default=1048576
        Read buffer chunk size in bytes.
    max_retries : int, default=3
        Retry attempts.

    Returns
    -------
    bool
        True if download succeeded, False otherwise.
    """
    headers = {
        "User-Agent": "Pharmacovigilance-FAERS-ETL/1.0 (Academic Research; Big Data Analytics ED547)"
    }
    temp_path = destination_path.with_suffix(".tmp")

    for attempt in range(1, max_retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as response, open(temp_path, "wb") as out_file:
                total_bytes_header = response.headers.get("Content-Length")
                total_bytes = int(total_bytes_header) if total_bytes_header else None
                if total_bytes is None and expected_size_mb:
                    total_bytes = int(expected_size_mb * 1024 * 1024)

                downloaded_bytes = 0
                start_time = time.time()
                last_print_time = 0.0

                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded_bytes += len(chunk)

                    current_time = time.time()
                    if current_time - last_print_time >= 0.5:
                        last_print_time = current_time
                        mb_done = downloaded_bytes / (1024 * 1024)
                        if total_bytes:
                            mb_total = total_bytes / (1024 * 1024)
                            pct = min(100.0, (downloaded_bytes / total_bytes) * 100.0)
                            bar_len = 25
                            filled = int(bar_len * pct / 100.0)
                            bar = "=" * filled + "-" * (bar_len - filled)
                            print(
                                f"\r    [{bar}] {pct:5.1f}% | {mb_done:6.1f} / {mb_total:6.1f} MB",
                                end="",
                                flush=True,
                            )
                        else:
                            print(f"\r    Downloaded {mb_done:6.1f} MB...", end="", flush=True)

                print()  # Newline after progress bar finishes
                elapsed = max(0.1, time.time() - start_time)
                speed_mb_s = (downloaded_bytes / (1024 * 1024)) / elapsed
                print(f"    Completed in {elapsed:.1f}s ({speed_mb_s:.2f} MB/s).")

            # Rename temp file to final target
            if destination_path.exists():
                destination_path.unlink()
            temp_path.rename(destination_path)
            return True

        except Exception as e:
            print(f"\n[ERROR] Download attempt {attempt}/{max_retries} failed for {url}: {e}")
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            if attempt < max_retries:
                time.sleep(attempt * 2)

    return False


def extract_zip(zip_path: Path, extract_to_dir: Path, remove_zip: bool = True) -> List[Path]:
    """Extract contents of a ZIP file into target directory.

    Parameters
    ----------
    zip_path : Path
        Path to source zip archive.
    extract_to_dir : Path
        Directory to extract files into.
    remove_zip : bool, default=True
        Whether to delete the zip file after successful extraction to save space.

    Returns
    -------
    list of Path
        List of extracted file paths.
    """
    extracted_files: List[Path] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            for member in archive.namelist():
                archive.extract(member, path=extract_to_dir)
                extracted_path = extract_to_dir / member
                extracted_files.append(extracted_path)
                print(f"    Extracted: {extracted_path.name} ({extracted_path.stat().st_size / (1024 * 1024):.2f} MB)")
        
        if remove_zip:
            zip_path.unlink()
            print(f"    Removed temporary archive: {zip_path.name}")
        return extracted_files
    except zipfile.BadZipFile as e:
        print(f"[ERROR] Corrupted ZIP file {zip_path.name}: {e}")
        if zip_path.exists():
            zip_path.unlink()
        return []


def download_faers(
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    max_files: Optional[int] = None,
    output_dir: Optional[Path] = None,
    keep_zip: bool = False,
    manifest_url: str = OPENFDA_MANIFEST_URL,
) -> List[Path]:
    """Execute the end-to-end bulk download and extraction pipeline for FAERS data.

    Parameters
    ----------
    start_year : int, optional
        Filter partitions starting from this year (inclusive).
    end_year : int, optional
        Filter partitions up to this year (inclusive).
    max_files : int, optional
        Limit number of partition files to download.
    output_dir : Path, optional
        Directory where extracted JSON files will be stored. Defaults to data/raw.
    keep_zip : bool, default=False
        If True, retains raw .zip files alongside extracted .json files.
    manifest_url : str, default=OPENFDA_MANIFEST_URL
        URL to fetch the OpenFDA manifest.

    Returns
    -------
    list of Path
        List of ready-to-ingest JSON files in output_dir.
    """
    target_dir = Path(output_dir) if output_dir else DEFAULT_RAW_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("FAERS Bulk Data Downloader")
    print(f"Destination Directory: {target_dir.resolve()}")
    print(f"Filters: start_year={start_year}, end_year={end_year}, max_files={max_files}")
    print("=" * 70)

    # 1. Fetch manifest
    manifest = fetch_manifest(manifest_url)

    # 2. Parse & filter partitions
    partitions = parse_drug_event_partitions(
        manifest=manifest,
        start_year=start_year,
        end_year=end_year,
        max_files=max_files,
    )

    if not partitions:
        print("[WARNING] No partitions matched the specified filters. Nothing to download.")
        return []

    completed_json_files: List[Path] = []

    # 3. Download and extract each partition
    for idx, partition in enumerate(partitions, start=1):
        file_url = partition["file"]
        size_mb = float(partition.get("size_mb", 0.0))
        display_name = partition.get("display_name", f"Partition {idx}")

        zip_filename = file_url.split("/")[-1]
        zip_path = target_dir / zip_filename
        # Standard OpenFDA convention: drug-event-0001-of-0030.json.zip -> drug-event-0001-of-0030.json
        expected_json_name = zip_filename.replace(".zip", "")
        expected_json_path = target_dir / expected_json_name

        print(f"\n[{idx}/{len(partitions)}] {display_name} (~{size_mb:.1f} MB)")

        # Check if already downloaded and extracted
        if expected_json_path.exists() and expected_json_path.stat().st_size > 0:
            print(f"    [SKIP] Already exists: {expected_json_name} ({expected_json_path.stat().st_size / (1024 * 1024):.2f} MB)")
            completed_json_files.append(expected_json_path)
            continue

        print(f"    Downloading {zip_filename}...")
        download_success = download_file_with_progress(
            url=file_url,
            destination_path=zip_path,
            expected_size_mb=size_mb,
        )

        if not download_success:
            print(f"    [FAIL] Could not download {zip_filename}. Skipping to next partition.")
            continue

        print(f"    Extracting {zip_filename}...")
        extracted = extract_zip(
            zip_path=zip_path,
            extract_to_dir=target_dir,
            remove_zip=not keep_zip,
        )
        completed_json_files.extend(extracted)

    print("\n" + "=" * 70)
    print(f"Download Pipeline Complete! {len(completed_json_files)} JSON dataset(s) ready in {target_dir.resolve()}")
    print("=" * 70)

    return completed_json_files


def main() -> None:
    """CLI entrypoint for FAERS bulk downloader."""
    parser = argparse.ArgumentParser(
        description="OpenFDA FAERS Adverse Event Bulk Downloader & Extractor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Earliest partition year to download (e.g. 2020)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Latest partition year to download (e.g. 2024)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum number of partition files to download (useful for development/testing)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_RAW_DIR),
        help="Target folder for extracted JSON files",
    )
    parser.add_argument(
        "--keep-zip",
        action="store_true",
        help="Retain raw .zip archives after extraction instead of deleting them",
    )

    args = parser.parse_args()

    try:
        download_faers(
            start_year=args.start_year,
            end_year=args.end_year,
            max_files=args.max_files,
            output_dir=Path(args.output_dir),
            keep_zip=args.keep_zip,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Download interrupted by user.")
        sys.exit(130)
    except Exception as err:
        print(f"\n[FATAL] Pipeline failed: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
