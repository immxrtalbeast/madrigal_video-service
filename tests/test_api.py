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

    status = None
    for _ in range(10):
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
