#!/usr/bin/env python3
"""Generate field-values JSON for each of the ten synthetic applicants."""

import json
import sys
from pathlib import Path

FIELD_INFO_PATH = Path(sys.argv[1])
APPLICANTS_PATH = Path(sys.argv[2])
OUTPUT_DIR = Path(sys.argv[3])

with open(FIELD_INFO_PATH) as f:
    field_info = json.load(f)

valid_ids = {fi["field_id"] for fi in field_info}
page_of = {fi["field_id"]: fi["page"] for fi in field_info}

with open(APPLICANTS_PATH) as f:
    applicants_data = json.load(f)

shared = applicants_data["shared_answers"]

for app in applicants_data["applicants"]:
    app_id = app["id"]

    if app["born_male_after_1959"]:
        ss_answer = "/Yes" if app["registered_selective_service"] else "/No"
    else:
        ss_answer = "/No"

    fields = [
        {"field_id": "Full Name", "page": page_of["Full Name"], "value": app["full_name"]},
        {"field_id": "PLACE OF BIRTH Include city and state or country", "page": page_of["PLACE OF BIRTH Include city and state or country"], "value": app["place_of_birth"]},
        {"field_id": "Country of Citizenship", "page": page_of["Country of Citizenship"], "value": "United States"},
        {"field_id": "Are you a U.S. Citizen?", "page": page_of["Are you a U.S. Citizen?"], "value": "/Yes"},
        {"field_id": "DATE OF BIRTH MM  DD  YYYY", "page": page_of["DATE OF BIRTH MM  DD  YYYY"], "value": app["date_of_birth"]},
        {"field_id": "Day", "page": page_of["Day"], "value": app["day_phone"]},
        {"field_id": "Other Names Used 2", "page": page_of["Other Names Used 2"], "value": app["other_names"]},
        {"field_id": "Male", "page": page_of["Male"], "value": "/Yes" if app["born_male_after_1959"] else "/No"},
        {"field_id": "Have you registered with Selective Service", "page": page_of["Have you registered with Selective Service"], "value": ss_answer},
        {"field_id": "Have you ever served in the U.S. Military", "page": page_of["Have you ever served in the U.S. Military"], "value": "/No"},
        {"field_id": "Have you been convicted imprisoned probation or paroled last 7 years", "page": page_of["Have you been convicted imprisoned probation or paroled last 7 years"], "value": "/No"},
        {"field_id": "Have you been court martialed in the last 7 years", "page": page_of["Have you been court martialed in the last 7 years"], "value": "/No"},
        {"field_id": "Are you currently under charges", "page": page_of["Are you currently under charges"], "value": "/No"},
        {"field_id": "Have you been fired or debarred or quit due to a specific problem or quit after being told you would be fired", "page": page_of["Have you been fired or debarred or quit due to a specific problem or quit after being told you would be fired"], "value": "/No"},
        {"field_id": "Are you delinquent on any Federal debt", "page": page_of["Are you delinquent on any Federal debt"], "value": "/No"},
        {"field_id": "Do any of your relatives work for the agency or government organization for which you are submitting this form", "page": page_of["Do any of your relatives work for the agency or government organization for which you are submitting this form"], "value": "/No"},
        {"field_id": "Have you applied or do you receive retirement or pension benefits from the military Federal or D.C. government", "page": page_of["Have you applied or do you receive retirement or pension benefits from the military Federal or D.C. government"], "value": "/No"},
        {"field_id": "Continuation of Space or Agency Specific Questions", "page": page_of["Continuation of Space or Agency Specific Questions"], "value": shared["continuation"]},
    ]

    for f in fields:
        assert f["field_id"] in valid_ids, f"Unknown field: {f['field_id']}"

    output_path = OUTPUT_DIR / f"{app_id}.json"
    with open(output_path, "w") as f:
        json.dump(fields, f, indent=2)
    print(f"Wrote {output_path} ({len(fields)} fields)")

print("Done generating all field-values files.")
