import csv
import glob
import json
import os
import sys

import boto3
import requests

IGNORE_LIST = (
    ".gif",
    "ashfield-dc.gov.uk/UserData/8/2/1/Info00000128/bigpic.jpg",
)


def make_face_dir(file_name):
    face_dir_name = "face_data"
    rk_json_path = file_name.replace("/json/", "/{}/".format(face_dir_name))
    dir_name = rk_json_path.split(face_dir_name)[0] + face_dir_name
    os.makedirs(dir_name, exist_ok=True)
    return rk_json_path


def detect_face(rk_json_path, photo_url, councillor_json):
    rekognition = boto3.client("rekognition", "eu-west-1")
    attributes = ["ALL"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
        "referer": councillor_json["url"],
    }

    print(photo_url)
    image = requests.get(photo_url, headers=headers)
    if (
        "Content-Length" in image.headers
        and int(image.headers["Content-Length"]) < 4000
    ):
        # this is tiny
        return

    detected = rekognition.detect_faces(
        Image={"Bytes": image.content}, Attributes=attributes
    )
    detected["councillor_json"] = councillor_json
    with open(rk_json_path, "w") as out:
        out.write(json.dumps(detected, indent=4))


for file_name in glob.glob("./data/**/json/*.jsonss"):
    with open(file_name) as f:
        json_data = json.loads(f.read())
        if "photo_url" in json_data:
            photo_url = json_data["photo_url"]
            if not photo_url.startswith("http"):
                continue
            if "http://modeste:9075" in photo_url:
                continue
            if "doncaster" in photo_url:
                continue
            if "https://www.democracy.caerphilly.gov.uk" in photo_url:
                photo_url = photo_url.replace("https", "http")
            if photo_url.endswith(IGNORE_LIST):
                continue
            rk_json_path = make_face_dir(file_name)
            if not os.path.exists(rk_json_path):
                try:
                    detect_face(rk_json_path, photo_url, json_data)
                except KeyboardInterrupt:
                    import sys

                    sys.exit()
                except Exception as e:
                    print(e)
                    pass
# process and report
out_csv = csv.DictWriter(
    open("councillors-with-gender-2021-06-07.csv", "w"),
    fieldnames=[
        "council_id",
        "name",
        "division",
        "party",
        "email",
        "url",
        "photo_url",
        "gender_from_name",
        "gender_from_photo",
        "age_low",
        "age_high",
        "smile",
        "glasses",
        "beard",
        "happy",
    ],
)
out_csv.writeheader()
for file_name in glob.glob("./data/**/face_data/*.json"):
    json_data = json.load(open(file_name))
    if not json_data["FaceDetails"]:
        continue
    face = json_data["FaceDetails"][0]
    council = file_name.split("/data/")[1].split("/")[0]
    row = {
        "council_id": council,
        "name": json_data["councillor_json"]["raw_name"],
        "division": json_data["councillor_json"]["raw_division"],
        "party": json_data["councillor_json"]["raw_party"],
        "email": json_data["councillor_json"]["email"],
        "url": json_data["councillor_json"]["url"],
        "photo_url": json_data["councillor_json"]["photo_url"],
        # "gender_from_name": gender_from_name(json_data["councillor_json"]["raw_name"]),  # Function not defined
        "gender_from_photo": face["Gender"]["Value"],
        "age_low": face["AgeRange"]["Low"],
        "age_high": face["AgeRange"]["High"],
        "smile": face["Smile"]["Value"],
        "glasses": face["Eyeglasses"]["Value"],
        "beard": face["Beard"]["Value"],
        "happy": any(
            x for x in face["Emotions"] if x["Type"] == "HAPPY" and x["Confidence"] > 70
        ),
    }
    out_csv.writerow(row)
