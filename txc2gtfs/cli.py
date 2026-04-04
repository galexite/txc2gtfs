import argparse
from collections.abc import Sequence
from pathlib import Path

from . import convert


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        "input",
        type=Path,
        nargs="+",
        help="Path to TransXChange XML files",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=Path.cwd() / "gtfs.zip",
        type=Path,
        help="Output path for the generated GTFS zip file",
    )

    args = parser.parse_args(argv)

    convert(args.input, args.output)


if __name__ == "__main__":
    main()
