#!/usr/bin/env python3
"""Fill a PDF with field values from a JSON file."""

import json
import sys
from pathlib import Path
from pypdf import PdfReader, PdfWriter


def main() -> None:
    pdf_path = Path(sys.argv[1])
    values_path = Path(sys.argv[2])
    output_path = Path(sys.argv[3])

    with open(values_path, encoding="utf-8") as f:
        fields = json.load(f)

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)

    if reader.get_fields():
        for item in fields:
            field_id = item.get("field_id", "")
            value = item.get("value", "")
            try:
                writer.update_page_form_field_values(
                    writer.pages[item.get("page", 1) - 1],
                    {field_id: value},
                    auto_regenerate=False,
                )
            except Exception:
                pass

    with open(output_path, "wb") as f:
        writer.write(f)


if __name__ == "__main__":
    main()
