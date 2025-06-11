import csv
import glob
import json
import sys

field_names = [
    "council_id",
    "raw_division",
    "raw_identifier",
    "email",
    "url",
    "raw_name",
    "raw_party",
    "photo_url",
    "standing_down",
    # 'glasses',
    # 'age_high',
    # 'smile',
    # 'happy',
    # 'age_low',
    # 'beard',
    # 'gender'
]

# field_names = [
#     'council_id',
#     'num_councilors',
# ]

csvout = csv.DictWriter(sys.stdout, fieldnames=field_names)

csvout.writeheader()

councillor_counter = {}

for file_name in glob.glob("./data/**/json/*.json"):
    councillor = json.load(open(file_name))
    council_id = file_name.split("/")[-3]
    for k in list(councillor.keys()):
        if k not in field_names:
            del councillor[k]
    councillor["council_id"] = council_id
    csvout.writerow(councillor)

    # council_id = file_name.split("/")[-3]
    # if not council_id in councillor_counter:
    #     councillor_counter[council_id] = 0
    # councillor_counter[council_id] += 1

    # with open(file_name) as f:
    #     json_data = json.loads(f.read())
    #     if not json_data["FaceDetails"]:
    #         continue
    #     face = json_data["FaceDetails"][0]
    #     out_dict = json_data['councillor_json']
    #     out_dict.update({
    #         "gender": face["Gender"]["Value"],
    #         "age_low": face["AgeRange"]["Low"],
    #         "age_high": face["AgeRange"]["High"],
    #         "smile": face["Smile"]["Value"],
    #         "glasses": face["Eyeglasses"]["Value"],
    #         "beard": face["Beard"]["Value"],
    #         "happy": any(
    #             [
    #                 x
    #                 for x in face["Emotions"]
    #                 if x["Type"] == "HAPPY" and x["Confidence"] > 70
    #             ]
    #         )
    #     })
    #     csvout.writerow(out_dict)

# for council_id, count in councillor_counter.items():
#     print(",".join((council_id, str(count))))
