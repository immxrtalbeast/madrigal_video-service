import base64
import time

from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_video_flow_with_local_queue():
    payload = {
        "idea": "Сделай промо ролик про новый курс",
        "duration_seconds": 30,
        "language": "ru",
    }
    create_resp = client.post("/videos", json=payload)
    assert create_resp.status_code == 202
    job_id = create_resp.json()["job"]["id"]

    draft_resp = client.get(f"/videos/{job_id}")
    assert draft_resp.status_code == 200
    draft_job = draft_resp.json()["job"]
    assert draft_job["stage"] == "draft_review"
    approval_payload = {
        "summary": draft_job.get("storyboard_summary"),
        "scenes": draft_job.get("storyboard"),
    }
    approve_resp = client.post(f"/videos/{job_id}/draft:approve", json=approval_payload)
    assert approve_resp.status_code == 200

    subtitles_text = None
    for _ in range(40):
        status_resp = client.get(f"/videos/{job_id}")
        assert status_resp.status_code == 200
        job_payload = status_resp.json()["job"]
        if job_payload["stage"] == "subtitle_review":
            subtitles_text = job_payload.get("subtitles_text") or "Fallback subtitle"
            break
        time.sleep(0.05)
    assert subtitles_text is not None

    subs_resp = client.post(
        f"/videos/{job_id}/subtitles:approve",
        json={"text": subtitles_text},
    )
    assert subs_resp.status_code == 200

    status = None
    for _ in range(40):
        status_resp = client.get(f"/videos/{job_id}")
        assert status_resp.status_code == 200
        status = status_resp.json()["job"]["status"]
        if status == "ready":
            break
        time.sleep(0.05)

    list_resp = client.get("/videos")
    assert list_resp.status_code == 200
    assert any(item["id"] == job_id for item in list_resp.json()["items"])

    expand_resp = client.post("/ideas:expand", json={"idea": "видео о пользе медитации"})
    assert expand_resp.status_code == 200
    assert "медитации" in expand_resp.json()["summary"]


def test_media_upload_and_list():
    payload = {
        "folder": "test",
        "filename": "demo.txt",
        "content_type": "text/plain",
        "data": base64.b64encode(b"demo-content").decode("ascii"),
    }
    headers = {"X-User-ID": "tester"}
    upload_resp = client.post("/media", json=payload, headers=headers)
    assert upload_resp.status_code == 201
    asset = upload_resp.json()["asset"]
    assert asset["key"].endswith("demo.txt")
    assert "tester" in asset["key"]

    list_resp = client.get("/media", params={"folder": "test"}, headers=headers)
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    assert any(item["key"].endswith("demo.txt") for item in items)
