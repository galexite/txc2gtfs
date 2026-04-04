from collections.abc import Iterator
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

GTFS_FILES = [
    "stops.txt",
    "stop_times.txt",
    "trips.txt",
    "calendar.txt",
    "routes.txt",
]


def export_to_zip(db: Path, output: Path, worker_output: Iterator[Path]) -> None:
    """Reads the gtfs database and generates an export dictionary for GTFS"""
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as zf:
        files = [(f"{f[:-3]}.csv", zf.open(f, "w")) for f in GTFS_FILES]
        first_worker = True
        for worker in worker_output:
            for in_path, out_file in files:
                with (worker / in_path).open("rb") as in_file:
                    buf = in_file.read(4096)
                    if not first_worker:
                        # Skip the header
                        buf = buf[buf.index(b"\n") :]
                    out_file.write(buf)
                    while buf := in_file.read(4096):
                        out_file.write(buf)

            if first_worker:
                first_worker = False
