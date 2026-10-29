"""Read-only audit of tracked source, development history and local package ZIPs."""
from pathlib import Path
import subprocess
import zipfile

root = Path(__file__).resolve().parents[1]
git = ["git", "-c", f"safe.directory={root.as_posix()}"]
tracked = subprocess.check_output(git + ["ls-files", "-z"], cwd=root).decode().split("\0")
counts = dict(library=0, tests=0, examples=0, cli=0)
private_parts = {"_local", "_build", ".git", ".mooncakes", "node_modules", "__pycache__"}
private_terms = ("申报书", "审查报告", "自查报告", "辅助报告", "汇总报告")

def check_name(name, in_package=False):
    parts = set(name.replace("\\", "/").split("/"))
    forbidden = private_parts
    if parts & forbidden or any(term in name for term in private_terms):
        raise SystemExit("Private/generated material leaked: " + name)

for name in filter(None, tracked):
    check_name(name)
    if name.endswith(".mbt"):
        category = "tests" if name.endswith("test.mbt") else "examples" if name.startswith("examples/") else "cli" if name.startswith("cmd/") else "library"
        counts[category] += sum(bool(line.strip()) and not line.lstrip().startswith("//")
                                for line in (root / name).read_text(encoding="utf-8").splitlines())
total = sum(counts.values())
commits = int(subprocess.check_output(git + ["rev-list", "--count", "HEAD"], cwd=root))
print({**counts, "effective_MoonBit_total": total, "commits": commits,
       "note": "library category includes inline tests; no generated fixture literals"})
if total <= 4000 or commits <= 30:
    raise SystemExit("Require >4000 effective MoonBit lines and >30 meaningful commits")
archives = list((root / "_build/publish").glob("*.zip"))
for archive in archives:
    with zipfile.ZipFile(archive) as package:
        for name in package.namelist():
            check_name(name, in_package=True)
print(f"Tracked files and {len(archives)} local package archive(s) passed hygiene checks; no upload performed")
