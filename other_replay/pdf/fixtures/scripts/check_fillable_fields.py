#!/usr/bin/env python3
"""Check and report fillable fields in a PDF."""

import sys
from pypdf import PdfReader


def main() -> None:
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read().strip()
    reader = PdfReader(pdf_path)
    fields = reader.get_fields() or {}

    leaf_fields = sum(
        not field.get("/Kids") and field.get("/FT") in {"/Tx", "/Sig", "/Ch"}
        for field in fields.values()
    )
    button_groups = sum(
        bool(field.get("/Kids")) and field.get("/FT") == "/Btn"
        for field in fields.values()
    )
    terminal_fields = leaf_fields + button_groups

    print(f"PDF: {pdf_path}")
    print(f"Pages: {len(reader.pages)}")
    print(f"Raw fields: {len(fields)}")
    print(f"Terminal fields (text/sig/ch + button groups): {terminal_fields}")
    print(f"Fillable: {'YES' if terminal_fields > 0 else 'NO'}")


if __name__ == "__main__":
    main()
