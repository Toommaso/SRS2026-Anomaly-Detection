"""
Downloader dataset CWRU (Case Western Reserve University Bearing Data)
Scarica file .mat e li converte in CSV pronti per il simulatore.

Uso:
    python download_cwru.py

Output: ./cwru_data/*.csv
Ogni CSV ha colonne: timestamp_offset_ms, vibration_drive_end, vibration_fan_end
"""

import os
import urllib.request
import numpy as np
import csv
from pathlib import Path

try:
    from scipy.io import loadmat
    SCIPY_OK = True
except ImportError:
    SCIPY_OK = False
    print("scipy non installato — installa con: pip install scipy")

OUTPUT_DIR = Path("cwru_data")

# File dataset CWRU — condizioni di guasto su cuscinetti
# Fonte: https://engineering.case.edu/bearingdatacenter/download-data-file
CWRU_FILES = {
    "normal_1797rpm": "https://engineering.case.edu/sites/default/files/Normal_1.mat",
    "bearing_inner_0007_1797rpm": "https://engineering.case.edu/sites/default/files/IR007_1.mat",
    "bearing_outer_0007_1797rpm": "https://engineering.case.edu/sites/default/files/OR007@6_1.mat",
    "bearing_ball_0007_1797rpm":  "https://engineering.case.edu/sites/default/files/B007_1.mat",
}

SAMPLE_RATE_HZ = 12_000  # il dataset CWRU è campionato a 12 kHz


def download_file(url: str, dest: Path) -> bool:
    if dest.exists():
        print(f"  già presente: {dest.name}")
        return True
    print(f"  scaricando {dest.name}...")
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"  ok: {dest.name} ({dest.stat().st_size // 1024} KB)")
        return True
    except Exception as e:
        print(f"  errore: {e}")
        return False


def mat_to_csv(mat_path: Path, csv_path: Path, label: str, max_samples: int = 60_000):
    """Converte un file .mat CWRU in CSV con timestamp e label."""
    if not SCIPY_OK:
        return
    mat = loadmat(str(mat_path))

    # I file CWRU hanno chiavi tipo 'X097_DE_time', 'X097_FE_time'
    de_key = next((k for k in mat if k.endswith("DE_time")), None)
    fe_key = next((k for k in mat if k.endswith("FE_time")), None)

    if de_key is None:
        print(f"  nessuna chiave DE trovata in {mat_path.name}, chiavi: {list(mat.keys())}")
        return

    de_signal = mat[de_key].flatten()[:max_samples]
    fe_signal = mat[fe_key].flatten()[:max_samples] if fe_key else np.zeros_like(de_signal)

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_offset_ms", "vibration_drive_end", "vibration_fan_end", "label"])
        for i, (de, fe) in enumerate(zip(de_signal, fe_signal)):
            ts_ms = round(i * 1000.0 / SAMPLE_RATE_HZ, 4)
            writer.writerow([ts_ms, round(float(de), 6), round(float(fe), 6), label])

    print(f"  convertito: {csv_path.name} ({len(de_signal)} campioni)")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    mat_dir = OUTPUT_DIR / "mat"
    mat_dir.mkdir(exist_ok=True)

    print("=== Download dataset CWRU ===")
    for label, url in CWRU_FILES.items():
        mat_path = mat_dir / f"{label}.mat"
        csv_path = OUTPUT_DIR / f"{label}.csv"

        ok = download_file(url, mat_path)
        if ok and SCIPY_OK and not csv_path.exists():
            mat_to_csv(mat_path, csv_path, label)

    print("\n=== Riepilogo file CSV generati ===")
    for csv_file in sorted(OUTPUT_DIR.glob("*.csv")):
        size_kb = csv_file.stat().st_size // 1024
        print(f"  {csv_file.name:50s} {size_kb:>6} KB")

    print(f"\nDati pronti in: {OUTPUT_DIR.resolve()}")
    print("Imposta CWRU_DATA_DIR=./cwru_data nel simulatore per usarli.")


if __name__ == "__main__":
    main()
