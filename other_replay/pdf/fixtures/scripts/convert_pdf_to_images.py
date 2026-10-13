#!/usr/bin/env python3
"""Render PDF pages as PNG images using pdf2image."""

import sys
from pathlib import Path
from pdf2image import convert_from_path


def main() -> None:
    pdf_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)

    images = convert_from_path(str(pdf_path), dpi=150)
    for i, img in enumerate(images, start=1):
        out_path = output_dir / f"page_{i}.png"
        img.save(out_path, "PNG")
        print(f"Saved page {i} as {out_path} (size: {img.size})")


if __name__ == "__main__":
    main()
