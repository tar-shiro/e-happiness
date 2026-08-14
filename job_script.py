#!/usr/bin/env python
"""GitHub Actions job executor for the Gallery advanced-search relay.

Triggered by a repository_dispatch (`event_type: gallery-job-trigger`). Reads
the job id from the dispatch payload, pulls the one-time Pixiv refresh token
out of D1 `job_tokens` (and deletes that row immediately — 用完即焚), runs the
requested action via pixivpy3, and writes the results back into D1
`job_results` for the browser to poll.

Security notes:
  - The token is read from the dispatch payload's jobId, never from an env var,
    and is deleted from D1 the moment it is read. It is never printed.
  - Nothing in this script ever touches a file with a token in it.
  - CF_ACCOUNT_ID / CF_D1_DB_ID / CF_API_TOKEN come from workflow secrets.
"""

import json
import os
import random
import sys
import time
from urllib.parse import parse_qs, urlparse

import requests
from pixivpy3 import AppPixivAPI

_MAX_RETRIES = 2
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}
_TOTAL_BUDGET_SEC = 240  # hard cap for the whole page loop


class _TransientError(Exception):
    pass


def _pixiv_api_call(fn, *args, **kwargs):
    """Call a pixivpy3 method, retrying transient network errors only."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt >= _MAX_RETRIES:
                raise
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"  pixiv call failed ({e}), retrying in {wait:.1f}s...")
            time.sleep(wait)


# ---- D1 REST (same pattern as pixiv-daily.py) ----

def _d1_execute(sql, params=None):
    url = (
        f"https://api.cloudflare.com/client/v4/accounts/{os.environ['CF_ACCOUNT_ID'].strip()}"
        f"/d1/database/{os.environ['CF_D1_DB_ID'].strip()}/query"
    )
    payload = {"sql": sql}
    if params:
        payload["params"] = params
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {os.environ['CF_API_TOKEN'].strip()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"D1 HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    if not data.get("success"):
        raise RuntimeError("D1 error: " + "; ".join(e.get("message", str(e)) for e in data.get("errors", [])))
    return data


def _d1_select_rows(sql, params=None):
    """Run a SELECT and return the result rows (empty list if none)."""
    data = _d1_execute(sql, params)
    return (data.get("result") or [{}])[0].get("results") or []


# ---- pixivpy3 helpers ----

def _next_offset(next_url):
    """Extract the offset param from a next_url (None when exhausted)."""
    if not next_url:
        return None
    qs = parse_qs(urlparse(next_url).query)
    vals = qs.get("offset")
    if not vals:
        return None
    try:
        return int(vals[0])
    except ValueError:
        return None


def _to_dict(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "_data"):
        return obj._data
    return obj


def _extract_items(resp):
    """Normalize a pixivpy3 response to (items, next_offset), mirroring the
    worker's callPixivApi result extraction."""
    if resp is None:
        return [], None
    for attr in ("illusts", "novels"):
        val = getattr(resp, attr, None)
        if val is not None:
            return [_to_dict(i) for i in val], _next_offset(getattr(resp, "next_url", None))
    for attr in ("illust", "novel", "user", "ugoira_metadata"):
        val = getattr(resp, attr, None)
        if val is not None:
            return [_to_dict(val)], None
    raw = _to_dict(resp)
    if isinstance(raw, dict) and raw:
        return [raw], None
    return [], None


def _run_action(api, action, params):
    """Call the pixivpy3 method for `action`, passing only present params."""
    if action == "search_illust":
        kwargs = {
            "word": params["word"],
            "search_target": params.get("search_target"),
            "sort": params.get("sort"),
            "duration": params.get("duration"),
            "start_date": params.get("start_date"),
            "end_date": params.get("end_date"),
            "offset": params.get("offset"),
        }
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        if params.get("type"):
            try:
                return api.search_illust(type=params["type"], **kwargs)
            except TypeError:
                print("  note: this pixivpy3 has no `type` param — ignoring type filter")
                return api.search_illust(**kwargs)
        return api.search_illust(**kwargs)
    if action == "search_novel":
        kwargs = {
            "word": params["word"],
            "search_target": params.get("search_target"),
            "sort": params.get("sort"),
            "start_date": params.get("start_date"),
            "end_date": params.get("end_date"),
            "offset": params.get("offset"),
        }
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return api.search_novel(**kwargs)
    if action == "illust_ranking":
        return api.illust_ranking(mode=params["mode"], date=params.get("date"), offset=params.get("offset"))
    if action == "novel_ranking":
        return api.novel_ranking(mode=params["mode"], date=params.get("date"), offset=params.get("offset"))
    if action == "illust_detail":
        return api.illust_detail(params["illust_id"])
    if action == "novel_detail":
        return api.novel_detail(params["novel_id"])
    if action == "user_detail":
        return api.user_detail(params["user_id"])
    if action == "user_illusts":
        return api.user_illusts(params["user_id"], type=params.get("type") or "illust", offset=params.get("offset"))
    if action == "user_novels":
        return api.user_novels(params["user_id"], type=params.get("type") or "novel", offset=params.get("offset"))
    if action == "illust_bookmark_add":
        return api.illust_bookmark_add(params["illust_id"], restrict=params.get("restrict") or "public")
    if action == "illust_bookmark_delete":
        return api.illust_bookmark_delete(params["illust_id"])
    if action == "novel_bookmark_add":
        return api.novel_bookmark_add(params["novel_id"], restrict=params.get("restrict") or "public")
    if action == "ugoira_metadata":
        return api.ugoira_metadata(params["illust_id"])
    raise RuntimeError(f"unsupported action: {action}")


def _sort_results(items, sort_by, order):
    """Reproduce the worker's sortResults (type-stable so mixed types never
    raise a comparison error)."""
    if not sort_by or not items:
        return items

    def get_nested(obj, path):
        val = obj
        for part in path.split("."):
            if val is None:
                return None
            if isinstance(val, dict):
                val = val.get(part)
            else:
                try:
                    val = val[part]
                except (TypeError, KeyError, IndexError):
                    return None
        return val

    def key_fn(item):
        v = get_nested(item, sort_by)
        if isinstance(v, bool):
            return (2, 0, 0)
        if isinstance(v, (int, float)):
            return (0, v, "")
        if isinstance(v, str):
            return (1, 0, v)
        return (3, 0, "")

    return sorted(items, key=key_fn, reverse=(order == "desc"))


# ---- main ----

def main():
    event_path = os.environ.get("GITHUB_EVENT_PATH", "")
    if not event_path or not os.path.exists(event_path):
        raise SystemExit("GITHUB_EVENT_PATH not set — is this a repository_dispatch run?")
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)
    job_id = (event.get("client_payload") or {}).get("jobId")
    if not job_id:
        raise SystemExit("no jobId in dispatch payload")

    print(f"gallery-job: starting job {job_id}")
    try:
        # 1. One-time token: read then delete immediately (用完即焚)
        rows = _d1_select_rows("SELECT token FROM job_tokens WHERE job_id = ?", [job_id])
        if not rows:
            raise RuntimeError("no token row (expired or already consumed)")
        refresh_token = rows[0]["token"]
        _d1_execute("DELETE FROM job_tokens WHERE job_id = ?", [job_id])
        print("  token consumed and deleted")

        # 2. Job spec (action + params + pagination)
        prows = _d1_select_rows("SELECT params FROM job_results WHERE job_id = ?", [job_id])
        if not prows:
            raise RuntimeError("no job params")
        spec = json.loads(prows[0]["params"])
        action = spec["action"]
        params = spec["params"]
        pagination = spec.get("pagination") or {}
        pages = int(pagination.get("pages") or 1)
        sort_by = pagination.get("sort_by") or None
        sort_order = pagination.get("sort_order") or "desc"

        _d1_execute("UPDATE job_results SET status='running', updated_at=datetime('now') WHERE job_id=?", [job_id])
        print(f"  action={action} pages={pages}")

        # 3. Auth + run with pagination
        api = AppPixivAPI()
        _pixiv_api_call(api.auth, refresh_token=refresh_token)
        print("  pixiv auth ok")

        all_items = []
        next_offset = None
        page = 0
        warning = None
        start = time.time()
        while page < pages:
            if time.time() - start > _TOTAL_BUDGET_SEC:
                warning = f"timeout after {page} page(s)"
                break
            call_params = dict(params)
            if next_offset is not None:
                call_params["offset"] = next_offset
            resp = _pixiv_api_call(_run_action, api, action, call_params)
            items, next_offset = _extract_items(resp)
            all_items.extend(items)
            page += 1
            print(f"  page {page}: +{len(items)} (total {len(all_items)})")
            if not items or next_offset is None:
                break
            time.sleep(0.6 + random.uniform(0, 0.5))

        results = _sort_results(all_items, sort_by, sort_order)
        payload = {"results": results, "count": len(results), "pages": page}
        if warning:
            payload["warning"] = warning

        _d1_execute(
            "UPDATE job_results SET status='done', payload=?, updated_at=datetime('now') WHERE job_id=?",
            [json.dumps(payload, ensure_ascii=False), job_id],
        )
        print(f"  done: {len(results)} results over {page} page(s)")
    except SystemExit:
        raise
    except Exception as e:
        _mark_error(job_id, e)
        raise


def _mark_error(job_id, exc):
    try:
        _d1_execute(
            "UPDATE job_results SET status='error', error=?, updated_at=datetime('now') WHERE job_id=?",
            [str(exc)[:500], job_id],
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()
