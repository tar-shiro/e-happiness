#!/usr/bin/env python
"""GitHub Actions job executor for the Gallery advanced-search relay.

Triggered by a repository_dispatch (`event_type: gallery-job-trigger`). Reads
the job id from the dispatch payload, pulls the one-time Pixiv refresh token
out of D1 `job_tokens` (and deletes that row immediately — 用完即焚), runs the
requested action via pixivpy3, and writes the results back into D1
`job_results` for the browser to poll.
CREATE TABLE IF NOT EXISTS job_results (
  job_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,                -- pending | running | done | error
  params TEXT NOT NULL,                -- JSON {action, params, pagination}
  code_hash TEXT,                      -- 存取碼 SHA-256，job 所有權
  payload TEXT,                        -- JSON results（done 時）
  error TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_results_status ON job_results(status);

CREATE TABLE IF NOT EXISTS job_tokens (
  job_id TEXT PRIMARY KEY,
  token TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now'))
);
Security notes:
  - The token is read from the dispatch payload's jobId, never from an env var,
    and is deleted from D1 the moment it is read. It is never printed.
  - Nothing in this script ever touches a file with a token in it.
  - CF_ACCOUNT_ID / CF_D1_DB_ID / CF_API_TOKEN come from workflow secrets.

  IMPORTANT:THIS IS DEPLOYED SEPERATELY IN AN GITHUB REPO FROM THE MAIN SITE
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


def _set_progress(job_id, **kw):
    """Write a JSON progress snapshot to job_results.progress (best-effort).

    The column is added by a one-off D1 migration
    (`ALTER TABLE job_results ADD COLUMN progress TEXT`); until that migration
    runs on the live DB this silently no-ops so old deployments keep working.

    Frequency note: called at most once per fetched page, nowhere near D1's
    write limits. The frontend polls every 3s, so sub-page granularity would
    be invisible anyway.
    """
    try:
        _d1_execute(
            "UPDATE job_results SET progress=?, updated_at=datetime('now') WHERE job_id=?",
            [json.dumps(kw, ensure_ascii=False), job_id],
        )
    except Exception:
        pass


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
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if hasattr(obj, "dict"):
        return obj.dict()
    return obj


def _auth_user_profile(api, auth_resp=None):
    """Extract the authenticated user dict. pixivpy3's auth() does NOT store the
    profile on the api object (it sets self.user_id and returns the token
    response whose .response.user holds it), so reading `api.user` alone always
    comes back empty. Prefer the auth response, then api.user, then a
    user_detail fetch as fallbacks (best-effort)."""
    try:
        if auth_resp is not None:
            resp_user = getattr(getattr(auth_resp, "response", None), "user", None)
            if resp_user is not None:
                return _to_dict(resp_user)
        raw = getattr(api, "user", None)
        if raw is not None:
            return _to_dict(raw)
        uid = getattr(api, "user_id", None)
        if uid:
            detail = _pixiv_api_call(api.user_detail, uid)
            detail_user = getattr(detail, "user", None)
            if detail_user is not None:
                return _to_dict(detail_user)
    except Exception:
        pass
    return None


def _extract_items(resp):
    """Normalize a pixivpy3 response to (items, next_offset), mirroring the
    worker's callPixivApi result extraction."""
    if resp is None:
        return [], None
    for attr in ("illusts", "novels"):
        val = getattr(resp, attr, None)
        if val is not None:
            return [_to_dict(i) for i in val], _next_offset(getattr(resp, "next_url", None))
    val = getattr(resp, "user_previews", None)
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
    if action == "search_user":
        return api.search_user(params["user_name"], offset=params.get("offset"))
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


# ---- Multi-keyword search (each keyword its own pixiv call) ----
#
# pixiv's search API takes one `word` + one `search_target` per call, so
# multi-keyword is N independent native calls (each keyword row in the UI sends
# its own word/search_target). Results are merged by id and each item is tagged
# with `qmatch` — the sorted list of query indices that matched it — so the
# frontend can combine AND/OR (and exclude) client-side without re-querying.

def _item_id(item):
    return item.get("id") or item.get("illust_id") or item.get("novel_id")


def _run_keyword_queries(api, action, queries, pages, budget_limit, progress_cb=None):
    """Run each {word, search_target} query as one native pixiv call, merge
    results by id, and tag every item with its `qmatch` list. `queries` is a
    list of dicts from the frontend (word + optional search_target). Returns
    the merged, deduped item list. progress_cb(**fields) is invoked once per
    fetched page so the UI can show real progress while it waits."""
    method = getattr(api, action)
    merged = {}   # id -> item
    qmatch = {}   # id -> set of query indices
    start = time.time()
    for qi, q in enumerate(queries):
        word = (q.get("word") or "").strip()
        if not word:
            continue
        target = q.get("search_target") or "partial_match_for_tags"
        next_offset = None
        page = 0
        while page < pages:
            if time.time() - start > budget_limit:
                break
            call_params = {"word": word, "search_target": target}
            if next_offset is not None:
                call_params["offset"] = next_offset
            if q.get("sort"):
                call_params["sort"] = q["sort"]
            if q.get("start_date"):
                call_params["start_date"] = q["start_date"]
            if q.get("end_date"):
                call_params["end_date"] = q["end_date"]
            if action == "search_illust" and q.get("type"):
                call_params["type"] = q["type"]
            resp = _pixiv_api_call(method, **call_params)
            items, next_offset = _extract_items(resp)
            for item in items:
                key = _item_id(item)
                if key is None:
                    continue
                if key not in merged:
                    merged[key] = item
                    qmatch[key] = set()
                qmatch[key].add(qi)
            page += 1
            print(f"  kw[{qi}] '{word}' page {page}: +{len(items)} (merged {len(merged)})")
            if progress_cb:
                progress_cb(phase="fetching", kw=qi + 1, kws=len(queries),
                            kw_page=page, pages=pages, merged=len(merged))
            if not items or next_offset is None:
                break
            time.sleep(0.8 + random.uniform(0, 0.7))
        if time.time() - start > budget_limit:
            break
    for key, item in merged.items():
        item["qmatch"] = sorted(qmatch[key])
    return list(merged.values())


def _normalize_user_items(items):
    """search_user returns user_previews; reduce each to the user identity the
    frontend needs (id + name/account/avatar). avatar is put in
    profile_image_urls so the worker's /liveimg decorator signs it."""
    out = []
    for preview in items:
        u = preview.get("user") or {}
        av = (u.get("profile_image_urls") or {})
        avatar = av.get("px_170x170") or av.get("px_50x50") or av.get("px_16x16") or ""
        uid = str(u.get("id") or "")
        if not uid:
            continue
        profile = {}
        if avatar:
            profile = {"medium": avatar, "square_medium": avatar, "px_50x50": avatar}
        out.append({
            "user_id": uid,
            "id": uid,
            "account": u.get("account") or "",
            "name": u.get("name") or "",
            "artist_name": "@" + (u.get("account") or ""),
            "profile_image_urls": profile,
            "type": "user",
            "tags": [],
        })
    return out


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
        # Runner timeout is 10 min (600 s). Scale the page-loop budget with the
        # page count so large ranges don't truncate early; cap at 8 min to leave
        # setup/auth headroom. An explicit budget_sec from the worker overrides.
        budget_sec = int(pagination.get("budget_sec") or 0)
        budget_limit = budget_sec if budget_sec > 0 else min(480, max(180, pages * 5))

        _d1_execute("UPDATE job_results SET status='running', updated_at=datetime('now') WHERE job_id=?", [job_id])
        print(f"  action={action} pages={pages}")

        # 3. Auth + run with pagination
        api = AppPixivAPI()
        auth_resp = _pixiv_api_call(api.auth, refresh_token=refresh_token)
        print("  pixiv auth ok")
        _set_progress(job_id, phase="auth")

        # 3b. Backfill the owning gallery user's public profile — the worker can't
        # reach Pixiv (CF egress 403), so this run is the only place the identity
        # is confirmed. A bad/expired token raises in api.auth() above and the job
        # errors with Pixiv's own message, surfaced in the poll UI.
        gallery_user_id = params.get("gallery_user_id")
        user_profile = _auth_user_profile(api, auth_resp)
        if gallery_user_id and user_profile:
            try:
                avatar = (user_profile.get("profile_image_urls") or {}).get("px_50x50") or ""
                _d1_execute(
                    "UPDATE gallery_users SET pixiv_user_id=?, pixiv_account=?, pixiv_name=?, pixiv_avatar=?, last_used_at=datetime('now') WHERE id=?",
                    [str(user_profile.get("id") or ""), user_profile.get("account") or "",
                     user_profile.get("name") or "", avatar, gallery_user_id],
                )
                print("  profile backfilled")
            except Exception as e:
                print(f"  profile backfill skipped: {e}")

        if action == "whoami":
            payload = {"results": [], "user": user_profile or {}, "count": 0, "pages": 0}
            _d1_execute(
                "UPDATE job_results SET status='done', payload=?, updated_at=datetime('now') WHERE job_id=?",
                [json.dumps(payload, ensure_ascii=False), job_id],
            )
            print("  whoami done")
            return

        # Multi-keyword search: each {word, search_target} in `queries` runs as its
        # own native pixiv call (the native API accepts ONE word + ONE search_target),
        # items are merged and tagged with `qmatch` so the frontend judges AND/OR.
        # Plain single-field searches keep the offset-pagination loop.
        queries = params.get("queries")

        warning = None
        if action in ("search_illust", "search_novel") and isinstance(queries, list) and len(queries) > 0:
            all_items = _run_keyword_queries(api, action, queries, pages, budget_limit,
                                             progress_cb=lambda **f: _set_progress(job_id, **f))
            page = pages
            print(
                f"  keyword search: {len(all_items)} merged results over {len(queries)} query rows"
            )
        else:
            all_items = []
            next_offset = None
            page = 0
            start = time.time()
            while page < pages:
                if time.time() - start > budget_limit:
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
                _set_progress(job_id, phase="fetching", page=page, pages=pages,
                              total=len(all_items))
                if not items or next_offset is None:
                    break
                time.sleep(0.8 + random.uniform(0, 0.7))

        _set_progress(job_id, phase="finalizing", total=len(all_items))

        if action == "search_user":
            all_items = _normalize_user_items(all_items)
            print(f"  user results: {len(all_items)}")

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
