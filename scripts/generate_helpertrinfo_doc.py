from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT / "qry" / "config" / "helpertrinfo.dat"
OUT_PATH = ROOT / ".homestock_docu" / "helpertrinfo_catalog.md"


@dataclass
class HelperField:
    kind: str
    index: int
    name: str
    description: str = ""
    default: str = ""


@dataclass
class HelperTR:
    tr_code: str
    tr_name: str
    flags: list[str] = field(default_factory=list)
    fields: list[HelperField] = field(default_factory=list)


SECTION_LABEL = {
    "SI": "입력 Single",
    "SO": "출력 Single",
    "MO": "출력 Multi",
}


def parse_helpertrinfo(path: Path) -> list[HelperTR]:
    text = path.read_text("cp949", errors="ignore")
    blocks = re.findall(r"\*START,([^\r\n]+?)\r?\n(.*?)\*END", text, flags=re.S)
    trs: list[HelperTR] = []

    for header, body in blocks:
        header_parts = [part.strip() for part in header.split(",")]
        if len(header_parts) < 2:
            continue
        tr_code = header_parts[0]
        tr_name = header_parts[1]
        flags = header_parts[2:]
        tr = HelperTR(tr_code=tr_code, tr_name=tr_name, flags=flags)

        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 3:
                continue
            kind = parts[0]
            if kind not in SECTION_LABEL:
                continue
            try:
                index = int(parts[1])
            except ValueError:
                continue
            name = parts[2]
            description = parts[3] if len(parts) > 3 else ""
            default = parts[4] if len(parts) > 4 else ""
            tr.fields.append(
                HelperField(
                    kind=kind,
                    index=index,
                    name=name,
                    description=description,
                    default=default,
                )
            )

        trs.append(tr)

    return trs


def render(trs: list[HelperTR]) -> str:
    lines: list[str] = [
        "# helpertrinfo Catalog",
        "",
        f"- 원본 파일: `{HELPER_PATH}`",
        f"- TR 수: **{len(trs)}**",
        "- 구분:",
        "  - `SI` = 입력 Single",
        "  - `SO` = 출력 Single",
        "  - `MO` = 출력 Multi",
        "- 기준: `helpertrinfo.dat` 단독 파싱 결과",
        "",
    ]

    for tr in trs:
        lines.append(f"## {tr.tr_code}")
        lines.append("")
        lines.append(f"- 한글명: {tr.tr_name or '-'}")
        if tr.flags:
            lines.append(f"- 헤더 플래그: `{', '.join(tr.flags)}`")

        by_kind = {"SI": [], "SO": [], "MO": []}
        for field in tr.fields:
            by_kind[field.kind].append(field)

        for kind in ("SI", "SO", "MO"):
            fields = by_kind[kind]
            if not fields:
                continue
            lines.append(f"- {SECTION_LABEL[kind]}")
            lines.append("")
            lines.append("| Index | 필드명 | 설명 | 기본값/예시 |")
            lines.append("| --- | --- | --- | --- |")
            for field in fields:
                description = (field.description or "-").replace("|", "\\|")
                default = (field.default or "-").replace("|", "\\|")
                lines.append(f"| {field.index} | {field.name} | {description} | {default} |")
            lines.append("")

        lines.append("")

    return "\n".join(lines)


def main() -> None:
    trs = parse_helpertrinfo(HELPER_PATH)
    OUT_PATH.write_text(render(trs), encoding="utf-8")
    print(f"wrote {OUT_PATH}")
    print(f"trs {len(trs)}")


if __name__ == "__main__":
    main()
