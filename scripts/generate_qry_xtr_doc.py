from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QRY_DIR = ROOT / "qry"
OUT_PATH = ROOT / ".homestock_docu" / "qry_xtr_catalog.md"
HELPER_PATH = QRY_DIR / "config" / "helpertrinfo.dat"

NEW_MARK = b"\xff\xfe\xff"
STRUCT = {"Record", "RECORD", "CIONodeFolder", "CIONodeItem"}


@dataclass
class FieldInfo:
    kor: str = ""
    eng: str = ""
    example: str = "-"
    extra: str = "-"


@dataclass
class SectionInfo:
    name: str
    fields: list[FieldInfo] = field(default_factory=list)


@dataclass
class FileInfo:
    filename: str
    tr_code: str = ""
    tr_name: str = ""
    sections: list[SectionInfo] = field(default_factory=list)


@dataclass
class HelperField:
    section: str
    index: int
    kor: str
    desc: str = ""
    default: str = ""


@dataclass
class HelperTR:
    tr_code: str
    tr_name: str
    fields: list[HelperField] = field(default_factory=list)


def is_eng_identifier(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", text))


def is_meta(text: str) -> bool:
    return text in STRUCT or bool(re.fullmatch(r"Q[0-9A-Z]+", text)) or bool(re.fullmatch(r"\d+", text))


def is_korean_like(text: str) -> bool:
    return any("\uac00" <= ch <= "\ud7a3" for ch in text)


def is_description(text: str) -> bool:
    if not text or is_meta(text) or is_eng_identifier(text):
        return False
    if ":" in text:
        return True
    if "분리됨" in text:
        return True
    if "(" in text and ")" in text and len(text) >= 10:
        return True
    if "Default" in text or "미입력" in text:
        return True
    if re.match(r"^[0-9A-Za-z][\.:].+", text):
        return True
    if text.startswith(("'", '"', "#", "%", "&", "<", "(")):
        return True
    return False


def looks_like_label(text: str) -> bool:
    return (
        bool(text)
        and text != "추가"
        and not is_meta(text)
        and not is_description(text)
        and not is_eng_identifier(text)
    )


def acceptable_new_string(text: str) -> bool:
    text = text.strip()
    if not text:
        return False
    good = 0
    for ch in text:
        o = ord(ch)
        if ch in "\t\r\n":
            continue
        if 0x20 <= o <= 0x7E or 0xAC00 <= o <= 0xD7A3 or 0x3131 <= o <= 0x318E:
            good += 1
        elif ch in " ·:,_-/%&()[]{}+\"'=.#":
            good += 1
    return good / max(len(text), 1) >= 0.8


def extract_new_strings(data: bytes) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(data) - 4:
        if data[i : i + 3] == NEW_MARK:
            n = data[i + 3]
            j = i + 4
            chunk = data[j : j + 2 * n]
            if len(chunk) == 2 * n:
                try:
                    text = chunk.decode("utf-16le").strip()
                except UnicodeDecodeError:
                    text = ""
                if acceptable_new_string(text):
                    out.append(text)
                    i = j + 2 * n
                    continue
        i += 1
    return out


def extract_old_tokens(data: bytes) -> list[str]:
    parts = [p.decode("cp949", "ignore").strip() for p in re.split(rb"[\x00-\x08\x0b-\x1f]+", data)]
    return [p for p in parts if p]


def split_sections(tokens: list[str]) -> list[list[str]]:
    sections: list[list[str]] = []
    current: list[str] | None = None
    for tok in tokens:
        if tok == "Record":
            if current is not None:
                sections.append(current)
            current = [tok]
        elif current is not None:
            current.append(tok)
    if current is not None:
        sections.append(current)
    return sections


def attach_detail(field: FieldInfo, text: str) -> None:
    if field.example == "-":
        field.example = text
    elif field.extra == "-":
        field.extra = text
    else:
        field.extra = f"{field.extra}; {text}"


def parse_new_fields(section_tokens: list[str]) -> list[FieldInfo]:
    filtered = [t for t in section_tokens if t not in STRUCT and not is_meta(t)]
    fields: list[FieldInfo] = []
    pending_for_next: list[str] = []
    i = 0
    while i < len(filtered):
        tok = filtered[i]
        if tok == "추가":
            i += 1
            continue
        if looks_like_label(tok):
            field = FieldInfo(kor=tok)
            if i + 1 < len(filtered) and is_eng_identifier(filtered[i + 1]):
                field.eng = filtered[i + 1]
                i += 1
            if pending_for_next:
                for desc in pending_for_next:
                    attach_detail(field, desc)
                pending_for_next.clear()
            j = i + 1
            while j < len(filtered):
                nxt = filtered[j]
                if looks_like_label(nxt) and (j + 1 < len(filtered) and is_eng_identifier(filtered[j + 1])):
                    break
                if looks_like_label(nxt) and not is_description(nxt) and nxt != "추가":
                    break
                if is_description(nxt):
                    attach_detail(field, nxt)
                j += 1
            fields.append(field)
            i = j
            continue
        if is_description(tok):
            next_tok = filtered[i + 1] if i + 1 < len(filtered) else ""
            next_next_tok = filtered[i + 2] if i + 2 < len(filtered) else ""
            if looks_like_label(next_tok) and (is_eng_identifier(next_next_tok) or next_tok == "추가"):
                pending_for_next.append(tok)
            elif fields:
                attach_detail(fields[-1], tok)
            else:
                pending_for_next.append(tok)
            i += 1
            continue
        if is_eng_identifier(tok):
            fields.append(FieldInfo(eng=tok))
        i += 1
    return fields


def parse_old_fields(section_tokens: list[str]) -> list[FieldInfo]:
    filtered = [t for t in section_tokens if t not in STRUCT and not is_meta(t)]
    fields: list[FieldInfo] = []
    pending_for_next: list[str] = []
    for tok in filtered:
        if tok == "추가":
            continue
        if is_description(tok):
            pending_for_next.append(tok)
            continue
        if is_eng_identifier(tok):
            if fields and fields[-1].eng == "":
                fields[-1].eng = tok
            else:
                field = FieldInfo(eng=tok)
                if pending_for_next:
                    for desc in pending_for_next:
                        attach_detail(field, desc)
                    pending_for_next.clear()
                fields.append(field)
            continue
        if looks_like_label(tok):
            field = FieldInfo(kor=tok)
            if pending_for_next:
                for desc in pending_for_next:
                    attach_detail(field, desc)
                pending_for_next.clear()
            fields.append(field)
    return fields


def parse_new_file(path: Path) -> FileInfo:
    tokens = extract_new_strings(path.read_bytes())
    info = FileInfo(filename=path.name)
    if tokens:
        info.tr_code = tokens[0]
    if len(tokens) > 1:
        info.tr_name = tokens[1]
    sections = split_sections(tokens)
    for idx, section in enumerate(sections):
        name = "입력" if idx == 0 else "출력"
        fields = parse_new_fields(section)
        info.sections.append(SectionInfo(name=name, fields=fields))
    return info


def parse_old_file(path: Path) -> FileInfo:
    tokens = extract_old_tokens(path.read_bytes())
    info = FileInfo(filename=path.name)
    if tokens:
        info.tr_code = tokens[0]
    if len(tokens) > 1:
        info.tr_name = tokens[1]
    sections = split_sections(tokens)
    for idx, section in enumerate(sections):
        name = "입력" if idx == 0 else "출력"
        fields = parse_old_fields(section)
        info.sections.append(SectionInfo(name=name, fields=fields))
    return info


def is_new_format(path: Path) -> bool:
    data = path.read_bytes()
    return data.startswith(b"\x08\x02\xff\xfe\xff") or data.count(NEW_MARK) > 10


def parse_file(path: Path) -> FileInfo:
    return parse_new_file(path) if is_new_format(path) else parse_old_file(path)


def parse_helpertrinfo(path: Path) -> dict[str, HelperTR]:
    text = path.read_text("cp949", errors="ignore")
    blocks = re.findall(r"\*START,([^\r\n]+?)\r?\n(.*?)\*END", text, flags=re.S)
    helpers: dict[str, HelperTR] = {}
    for header, body in blocks:
        header_parts = [part.strip() for part in header.split(",")]
        if not header_parts:
            continue
        tr_code = header_parts[0]
        if not re.fullmatch(r"[A-Z0-9_]+", tr_code):
            continue
        tr_name = header_parts[1] if len(header_parts) > 1 else ""
        helper = HelperTR(tr_code=tr_code, tr_name=tr_name)
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 3:
                continue
            section_code = parts[0]
            if section_code not in {"SI", "SO", "MO"}:
                continue
            try:
                index = int(parts[1])
            except ValueError:
                continue
            kor = parts[2] if len(parts) > 2 else ""
            desc = parts[3] if len(parts) > 3 else ""
            default = parts[4] if len(parts) > 4 else ""
            section_name = "입력" if section_code == "SI" else "출력"
            helper.fields.append(
                HelperField(
                    section=section_name,
                    index=index,
                    kor=kor,
                    desc=desc,
                    default=default,
                )
            )
        helpers[tr_code] = helper
    return helpers


def merge_helper_into_file(info: FileInfo, helper: HelperTR | None) -> FileInfo:
    if helper is None:
        return info

    if helper.tr_name:
        info.tr_name = helper.tr_name

    input_fields = [field for section in info.sections if section.name == "입력" for field in section.fields]
    output_fields = [field for section in info.sections if section.name == "출력" for field in section.fields]
    section_map = {
        "입력": input_fields,
        "출력": output_fields,
    }

    helper_by_section: dict[str, list[HelperField]] = {"입력": [], "출력": []}
    for helper_field in helper.fields:
        helper_by_section.setdefault(helper_field.section, []).append(helper_field)

    for section_name, target_fields in section_map.items():
        helper_fields = sorted(helper_by_section.get(section_name, []), key=lambda item: item.index)
        cursor = 0
        used_indexes: set[int] = set()
        for helper_field in helper_fields:
            if cursor >= len(target_fields):
                break
            chosen_index = None
            if helper_field.kor:
                for candidate_index in range(cursor, len(target_fields)):
                    if candidate_index in used_indexes:
                        continue
                    if target_fields[candidate_index].kor == helper_field.kor:
                        chosen_index = candidate_index
                        break
            if chosen_index is None:
                for candidate_index in range(cursor, len(target_fields)):
                    if candidate_index in used_indexes:
                        continue
                    if not target_fields[candidate_index].kor or target_fields[candidate_index].kor == "-":
                        chosen_index = candidate_index
                        break
            if chosen_index is None:
                continue
            field = target_fields[chosen_index]
            if helper_field.kor:
                field.kor = helper_field.kor
            if helper_field.default:
                field.example = helper_field.default
            if helper_field.desc:
                if field.extra == "-":
                    field.extra = helper_field.desc
                elif helper_field.desc not in field.extra.split("; "):
                    field.extra = f"{helper_field.desc}; {field.extra}"
            used_indexes.add(chosen_index)
            cursor = chosen_index + 1

    merged_sections: list[SectionInfo] = []
    if input_fields:
        merged_sections.append(SectionInfo(name="입력", fields=input_fields))
    if output_fields:
        merged_sections.append(SectionInfo(name="출력", fields=output_fields))
    info.sections = merged_sections
    return info


def render(files: list[FileInfo]) -> str:
    lines: list[str] = []
    lines.append("# QRY XTR Catalog")
    lines.append("")
    lines.append(f"- 대상 폴더: `{QRY_DIR}`")
    lines.append(f"- 파일 수: **{len(files)}**")
    lines.append("- 기준: 프로젝트 폴더 아래 최신 `qry`의 `.xtr` 파일만 정리")
    lines.append("- 주의: 일부 구형 포맷 파일은 영문명/설명 정보가 비어 있을 수 있고, 일부 신형 포맷은 `추가` 같은 예약 필드가 포함된다.")
    lines.append("")

    for info in files:
        lines.append(f"## {info.filename}")
        lines.append("")
        lines.append(f"- TR코드: `{info.tr_code or '-'}`")
        lines.append(f"- 한글명: {info.tr_name or '-'}")
        merged_sections: list[SectionInfo] = []
        input_fields = [field for section in info.sections if section.name == "입력" for field in section.fields]
        output_fields = [field for section in info.sections if section.name == "출력" for field in section.fields]
        if input_fields:
            merged_sections.append(SectionInfo(name="입력", fields=input_fields))
        if output_fields:
            merged_sections.append(SectionInfo(name="출력", fields=output_fields))
        for section in merged_sections:
            lines.append(f"- {section.name}")
            lines.append("")
            lines.append("| Index | 한글명 | 영문명 | 예시/기본값 | 부가정보 |")
            lines.append("| --- | --- | --- | --- | --- |")
            for idx, field in enumerate(section.fields, start=1):
                kor = field.kor or "-"
                eng = field.eng or "-"
                example = (field.example or "-").replace("|", "\\|")
                extra = (field.extra or "-").replace("|", "\\|")
                lines.append(f"| {idx} | {kor} | {eng} | {example} | {extra} |")
            lines.append("")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    helpers = parse_helpertrinfo(HELPER_PATH) if HELPER_PATH.exists() else {}
    files = []
    for path in sorted(QRY_DIR.glob("*.xtr")):
        info = parse_file(path)
        files.append(merge_helper_into_file(info, helpers.get(info.tr_code)))
    OUT_PATH.write_text(render(files), encoding="utf-8")
    print(f"wrote {OUT_PATH}")
    print(f"files {len(files)}")


if __name__ == "__main__":
    main()
