#!/bin/bash
# Batch fill and render driver - executes exactly once per applicant.
# Runs from skill scripts directory so fill_fillable_fields.py's import works.
set -euo pipefail

BASE=/root/.openclaw/workspace/tool-modeling/SUB-MEM-PDF-01
SCRIPTS=/opt/pdf/scripts
INPUT=$BASE/input
OUTPUT=$BASE/output
FIELD_VALUES=$OUTPUT/field_values
FILLED=$OUTPUT/filled
RENDERED=$OUTPUT/rendered

cd "$SCRIPTS"

echo "=== BATCH FILL: 10 applicants ==="
for i in $(seq -w 1 10); do
    app="applicant_${i}"
    echo "Filling $app..."
    python3 fill_fillable_fields.py \
        "$INPUT/of306_aug2023.pdf" \
        "$FIELD_VALUES/${app}.json" \
        "$FILLED/${app}.pdf"
done

echo ""
echo "=== BATCH RENDER: 10 filled PDFs ==="
for i in $(seq -w 1 10); do
    app="applicant_${i}"
    echo "Rendering $app..."
    python3 convert_pdf_to_images.py \
        "$FILLED/${app}.pdf" \
        "$RENDERED/${app}/"
done

echo ""
echo "=== ALL DONE ==="
