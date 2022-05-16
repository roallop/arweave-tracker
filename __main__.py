import fire

from tracker import Tracker


# remove useless tags and flatten
def transform_tags(tags: list[dict]) -> dict:
    result = {}
    for item in tags:
        if item["name"] in {"App-Name", "Content-Type", "content-digest"}:
            continue
        result[item["name"].lower()] = item["value"]
    return result


if __name__ == "__main__":
    tracker = Tracker(
        tags=[{"name": "App-Name", "values": ["MirrorXYZ"]}], transformer=transform_tags
    )
    fire.Fire(tracker)
