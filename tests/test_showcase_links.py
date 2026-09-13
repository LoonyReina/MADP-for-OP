import re
from pathlib import Path
from urllib.parse import unquote


def test_showcase_local_links_resolve():
    root = Path(__file__).resolve().parents[1]
    pages = [root / "README.md", root / "README.zh-CN.md",
             root / "docs/architecture/FILE_FIRST_COLLABORATION.md"]
    pages += list((root / "docs/guides").glob("*.md"))
    pages += list((root / "docs/showcase").glob("*.md"))
    for page in pages:
        for target in re.findall(r"\]\(([^)]+)\)", page.read_text(encoding="utf-8")):
            if "://" in target or target.startswith("#"):
                continue
            path = (page.parent / unquote(target.split("#")[0])).resolve()
            assert path.is_relative_to(root) and path.is_file(), (page, target)
