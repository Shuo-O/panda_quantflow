import logging
import sys
from pathlib import Path


def _ensure_external_repo(repo_name: str) -> None:
    """
    Ensure the sibling mono-repository that hosts the requested package is on sys.path.

    Many of our internal packages (e.g. panda_factor) live as neighbouring repositories
    during local development. When the package is not installed into the environment we
    still want imports such as ``import panda_factor`` to succeed. If we detect the repo
    alongside this project we append it to sys.path.
    """
    try:
        repo_root = Path(__file__).resolve().parents[2].parent / repo_name
        if repo_root.exists():
            candidate_paths = [repo_root, repo_root / repo_name]
            for path in candidate_paths:
                if path.exists():
                    str_path = str(path)
                    if str_path not in sys.path:
                        sys.path.append(str_path)
    except Exception as exc:
        logging.getLogger(__name__).debug(
            "Failed to extend sys.path for repo %s: %s", repo_name, exc
        )


_ensure_external_repo("panda_factor")

from .base import BaseWorkNode, work_node, ui

__all__ = ["BaseWorkNode", "work_node", "ui"]
