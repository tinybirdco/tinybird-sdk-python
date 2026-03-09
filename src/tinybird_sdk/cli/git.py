from __future__ import annotations

import os
import re
import subprocess


def _get_branch_from_ci_env() -> str | None:
    if os.getenv("VERCEL_GIT_COMMIT_REF"):
        return os.environ["VERCEL_GIT_COMMIT_REF"]

    github_branch = os.getenv("GITHUB_HEAD_REF") or os.getenv("GITHUB_REF_NAME")
    if github_branch:
        return github_branch

    if os.getenv("CI_COMMIT_BRANCH"):
        return os.environ["CI_COMMIT_BRANCH"]
    if os.getenv("CIRCLE_BRANCH"):
        return os.environ["CIRCLE_BRANCH"]
    if os.getenv("BUILD_SOURCEBRANCHNAME"):
        return os.environ["BUILD_SOURCEBRANCHNAME"]
    if os.getenv("BITBUCKET_BRANCH"):
        return os.environ["BITBUCKET_BRANCH"]
    if os.getenv("GIT_BRANCH"):
        return os.environ["GIT_BRANCH"].removeprefix("origin/")
    if os.getenv("TRAVIS_BRANCH"):
        return os.environ["TRAVIS_BRANCH"]
    return None


def get_current_git_branch() -> str | None:
    try:
        branch = (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL)
            .decode("utf-8")
            .strip()
        )
        if branch == "HEAD":
            return _get_branch_from_ci_env()
        return branch
    except Exception:
        return _get_branch_from_ci_env()


def is_main_branch() -> bool:
    branch = get_current_git_branch()
    return branch in {"main", "master"}


def is_git_repo() -> bool:
    try:
        subprocess.check_output(["git", "rev-parse", "--git-dir"], stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False


def get_git_root() -> str | None:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "--show-toplevel"], stderr=subprocess.DEVNULL)
            .decode("utf-8")
            .strip()
        )
    except Exception:
        return None


def sanitize_branch_name(branch_name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", branch_name)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_")


def get_tinybird_branch_name() -> str | None:
    branch = get_current_git_branch()
    if not branch:
        return None
    sanitized = sanitize_branch_name(branch)
    return sanitized or None
