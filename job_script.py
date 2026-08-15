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


# ---- Compound search (multi-keyword / tags / excludes / match) ----
#
# pixiv's API only takes one `word` string; space-separated words in it already
# do native AND (for partial_match_for_tags). So we only do our own work when
# the combination can't be expressed in one call. The logic is split into three
# independent switches + two exclude fields:
#   - word_match  ('and'|'or'): how multiple keywords in `word` combine.
#     AND → one native space-AND call; OR → one call per keyword, union. OR
#     capped at 5 terms.
#   - tags_match  ('and'|'or'): same, for the tags field. AND → one native
#     space-AND call; OR → one call per tag (partial_match_for_tags), capped 5.
#   - cross_match ('all'|'any'): keyword-group × tag-group. all → intersect by
#     id; any → union + dedupe.
#   - exclude      : excluded keywords, post-filter over title/tags/artist.
#   - exclude_tags : excluded tags, post-filter over the item tag list only.

def _item_id(item):
    return item.get("id") or item.get("illust_id") or item.get("novel_id")


def _matches_exclude(item, exclude_terms):
    text = " ".join([
        item.get("title") or "",
        item.get("name") or "",
        " ".join((t.get("name") or "") for t in (item.get("tags") or [])),
        (item.get("user") or {}).get("name") or "",
    ]).lower()
    return any(term in text for term in exclude_terms)


def _matches_exclude_tags(item, exclude_tags):
    item_tags = " ".join((t.get("name") or "") for t in (item.get("tags") or [])).lower()
    return any(t in item_tags for t in exclude_tags)


def _build_query_groups(params):
    """Return [(group, [search_kwargs...]), ...], group in ('word','tags').
    Each kwargs dict is one pixiv call. Per field: AND → single native
    space-AND call; OR → one call per term, capped at 5."""
    word = (params.get("word") or "").strip()
    tags = (params.get("tags") or "").strip()
    chosen = params.get("search_target") or "partial_match_for_tags"
    common = {k: params[k] for k in ("sort", "start_date", "end_date") if params.get(k)}
    groups = []
    if word:
        wm = (params.get("word_match") or "and")
        if wm == "or":
            qs = [dict(common, word=t, search_target=chosen) for t in word.split()[:5]]
        else:
            qs = [dict(common, word=word, search_target=chosen)]
        groups.append(("word", qs))
    if tags:
        tm = (params.get("tags_match") or "and")
        if tm == "or":
            qs = [dict(common, word=t, search_target="partial_match_for_tags") for t in tags.split()[:5]]
        else:
            qs = [dict(common, word=tags, search_target="partial_match_for_tags")]
        groups.append(("tags", qs))
    return groups


def _combine_group(per_query, mode):
    """Combine a group's per-query item lists. AND → intersect by id across all
    its queries; OR → union + dedupe."""
    if not per_query:
        return []
    if mode == "or" or len(per_query) == 1:
        merged = []
        seen = set()
        for lst in per_query:
            for i in lst:
                key = _item_id(i)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(i)
        return merged
    keep = {_item_id(i) for i in per_query[0]}
    for lst in per_query[1:]:
        keep &= {_item_id(i) for i in lst}
    return [i for i in per_query[0] if _item_id(i) in keep]


def _run_compound_search(api, action, params, pages, budget_limit):
    """Run each field's query set, combine per-field (word_match / tags_match),
    cross-combine (cross_match), then apply both exclude filters. Returns the
    combined item list."""
    method = getattr(api, action)
    exclude_terms = (params.get("exclude") or "").lower().split()
    exclude_tags = (params.get("exclude_tags") or "").lower().split()
    cross_match = params.get("cross_match") or "all"
    groups = _build_query_groups(params)
    group_results = {}
    start = time.time()
    for group, queries in groups:
        per_query = []
        for q in queries:
            lst = []
            next_offset = None
            page = 0
            while page < pages:
                if time.time() - start > budget_limit:
                    break
                call_params = dict(q)
                if next_offset is not None:
                    call_params["offset"] = next_offset
                resp = _pixiv_api_call(method, **call_params)
                items, next_offset = _extract_items(resp)
                lst.extend(items)
                page += 1
                if not items or next_offset is None:
                    break
                time.sleep(0.8 + random.uniform(0, 0.7))
            per_query.append(lst)
            if time.time() - start > budget_limit:
                break
        group_results[group] = _combine_group(per_query, params.get(group + "_match") or "and")
        if time.time() - start > budget_limit:
            break

    if "word" in group_results and "tags" in group_results:
        if cross_match == "all":
            keep = {_item_id(i) for i in group_results["tags"]}
            merged = [i for i in group_results["word"] if _item_id(i) in keep]
        else:
            merged = []
            seen = set()
            for g in ("word", "tags"):
                for i in group_results[g]:
                    key = _item_id(i)
                    if key in seen:
                        continue
                    seen.add(key)
                    merged.append(i)
    elif "word" in group_results:
        merged = group_results["word"]
    else:
        merged = group_results.get("tags", [])

    if exclude_terms:
        merged = [i for i in merged if not _matches_exclude(i, exclude_terms)]
    if exclude_tags:
        merged = [i for i in merged if not _matches_exclude_tags(i, exclude_tags)]
    return merged


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

        # Compound search (multi-keyword / tags / excludes / match) needs several
        # pixiv calls combined server-side — the single-query loop can't express
        # it. Plain single-field searches keep the offset-pagination loop.
        is_compound = (
            action in ("search_illust", "search_novel")
            and bool(
                (params.get("tags") or "").strip()
                or (params.get("exclude") or "").strip()
                or (params.get("exclude_tags") or "").strip()
                or (params.get("word_match") or "and") != "and"
                or (params.get("tags_match") or "and") != "and"
                or (params.get("cross_match") or "all") != "all"
            )
        )

        warning = None
        if is_compound:
            all_items = _run_compound_search(api, action, params, pages, budget_limit)
            page = pages
            print(
                f"  compound: {len(all_items)} combined results "
                f"(word={params.get('word_match') or 'and'}, tags={params.get('tags_match') or 'and'}, "
                f"cross={params.get('cross_match') or 'all'})"
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
                if not items or next_offset is None:
                    break
                time.sleep(0.8 + random.uniform(0, 0.7))

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
