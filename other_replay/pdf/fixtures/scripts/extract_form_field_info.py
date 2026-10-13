#!/usr/bin/env python3
"""Extract fillable form-field info (id, page, rect) to JSON."""

import json
import sys
from pathlib import Path
from pypdf import PdfReader


def field_page(page_idx: int, field: dict) -> int:
    return page_idx + 1


def main() -> None:
    pdf_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    reader = PdfReader(str(pdf_path))
    fields = reader.get_fields() or {}
    result = []

    for page_idx, page in enumerate(reader.pages):
        for name, field in fields.items():
            try:
                if "/V" in field or "/FT" in field:
                    rect = None
                    if "/Rect" in field:
                        r = field["/Rect"]
                        if isinstance(r, (list, tuple)) and len(r) == 4:
                            rect = list(r)
                    result.append({
                        "field_id": name,
                        "type": str(field.get("/FT", "/Tx")),
                        "page": page_idx + 1,
                        "rect": rect,
                    })
            except Exception:
                pass

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Wrote {len(result)} fields to {output_path}")


if __name__ == "__main__":
    main()
