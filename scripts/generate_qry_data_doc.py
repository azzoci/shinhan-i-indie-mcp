from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "qry" / "data"
OUT_PATH = ROOT / ".homestock_docu" / "qry_data_catalog.md"


@dataclass
class MasterField:
    index: int
    field_type: int
    label: str
    fid: int
    offset: int
    length: int
    alias: str


@dataclass
class MasterTable:
    index: int
    name: str
    code: str
    filename: str
    fields: list[MasterField]

    @property
    def schema_width(self) -> int:
        return max((field.offset + field.length for field in self.fields), default=0)


def decode_cp949(data: bytes) -> str:
    return data.decode("cp949", errors="ignore")


def parse_mastertable(path: Path) -> list[MasterTable]:
    data = path.read_bytes()
    table_count, _ = struct.unpack_from("<II", data, 0)
    pos = 8
    tables: list[MasterTable] = []

    def find_header(start: int) -> tuple[int, int, str, str, str] | None:
        for off in range(max(8, start - 3), min(start + 16, len(data) - 16)):
            field_count = struct.unpack_from("<I", data, off)[0]
            if not (1 <= field_count <= 300):
                continue

            name_len = data[off + 4]
            if not (1 <= name_len <= 40):
                continue

            name_bytes = data[off + 5 : off + 5 + name_len]
            try:
                name = name_bytes.decode("cp949")
            except UnicodeDecodeError:
                continue

            cursor = off + 5 + name_len
            code_len = data[cursor]
            if not (1 <= code_len <= 10):
                continue

            code_bytes = data[cursor + 1 : cursor + 1 + code_len]
            if any(ch < 0x20 or ch > 0x7E for ch in code_bytes):
                continue
            code = code_bytes.decode("ascii", errors="ignore")

            cursor += 1 + code_len
            file_len = data[cursor]
            if not (4 <= file_len <= 20):
                continue

            file_bytes = data[cursor + 1 : cursor + 1 + file_len]
            if not file_bytes.lower().endswith(b".dat"):
                continue
            filename = file_bytes.decode("ascii", errors="ignore")
            return off, field_count, name, code, filename
        return None

    for table_index in range(1, table_count + 1):
        while pos < len(data) and data[pos] == 0:
            pos += 1

        header = find_header(pos)
        if header is None:
            raise ValueError(f"mastertable.fmt 파싱 실패: table_index={table_index}, pos={pos}")

        off, field_count, name, code, filename = header
        pos = off + 4 + 1 + len(name.encode("cp949")) + 1 + len(code) + 1 + len(filename)

        fields: list[MasterField] = []
        for field_index in range(1, field_count + 1):
            field_type = data[pos]
            pos += 1
            label_len = data[pos]
            pos += 1
            label = decode_cp949(data[pos : pos + label_len])
            pos += label_len
            fid, offset, length = struct.unpack_from("<III", data, pos)
            pos += 12
            alias_len = data[pos]
            pos += 1
            alias = data[pos : pos + alias_len].decode("ascii", errors="ignore")
            pos += alias_len

            fields.append(
                MasterField(
                    index=field_index,
                    field_type=field_type,
                    label=label,
                    fid=fid,
                    offset=offset,
                    length=length,
                    alias=alias,
                )
            )

        tables.append(
            MasterTable(
                index=table_index,
                name=name,
                code=code,
                filename=filename,
                fields=fields,
            )
        )

    return tables


def count_lines(raw: bytes) -> int:
    return len(raw.splitlines())


def first_line_bytes(raw: bytes) -> bytes:
    if b"\r\n" in raw:
        return raw.split(b"\r\n", 1)[0]
    if b"\n" in raw:
        return raw.split(b"\n", 1)[0]
    return raw


def is_zip_file(raw: bytes) -> bool:
    return raw.startswith(b"PK\x03\x04")


def load_dat_payload(path: Path) -> tuple[bytes, str]:
    raw = path.read_bytes()
    if not is_zip_file(raw):
        return raw, "plain"

    with ZipFile(path) as archive:
        names = archive.namelist()
        if not names:
            return b"", "zip-empty"
        inner = archive.read(names[0])
        return inner, f"zip:{names[0]}"


def summarize_text_file(path: Path) -> tuple[int, int, str, str]:
    raw, storage = load_dat_payload(path)
    sample_bytes = first_line_bytes(raw)
    sample_text = decode_cp949(sample_bytes).strip()
    return count_lines(raw), len(sample_bytes), sample_text, storage


def infer_extra_file(name: str, sample: str) -> str:
    if name == "market_day.dat":
        return "영업일 캘린더. 한 줄에 YYYYMMDD 하나씩 들어가는 단순 날짜 목록입니다."
    if name == "elwissue.dat":
        return "ELW 발행사 코드표. 앞 2자리는 코드, 뒤는 증권사명으로 보입니다."
    if name == "grpinfo.dat":
        return "종목별 그룹/대표명 보조 테이블로 보입니다. `단축코드 + 그룹명/표시명` 형태가 반복됩니다."
    if name == "kssjcod1.dat":
        return "테마/업종 소분류 코드 마스터로 보입니다. 앞쪽 숫자는 테마 코드, 뒤는 테마명입니다."
    if name == "kssjcod3.dat":
        return "테마 코드와 종목코드를 연결하는 매핑 테이블로 보입니다."
    if name == "nssbasfd.dat":
        return "주식 계열 마스터의 별도 버전으로 보이지만 `mastertable.fmt`에는 미등록입니다. 표준코드/단축코드/종목명이 앞부분에 반복됩니다."
    return f"스키마 미등록 보조 파일입니다. 첫 행 샘플: `{sample}`"


def parse_mfeed_appendix(path: Path) -> tuple[int, list[str]]:
    lines = path.read_text("cp949", errors="ignore").splitlines()
    nonempty = [line for line in lines if line.strip()]
    return len(nonempty), nonempty[:10]


def parse_pc_mapper(path: Path) -> tuple[int, list[tuple[str, int, list[str]]]]:
    text = path.read_text("cp949", errors="ignore")
    sections: list[tuple[str, int, list[str]]] = []
    current_name = ""
    current_entries: list[str] = []

    def flush() -> None:
        nonlocal current_name, current_entries
        if current_name:
            sections.append((current_name, len(current_entries), current_entries[:3]))
        current_name = ""
        current_entries = []

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        match = re.match(r"^\[(.+?)\]", line)
        if match:
            flush()
            current_name = match.group(1)
            continue
        if current_name and line and not line.lstrip().startswith("#"):
            current_entries.append(line)
    flush()
    return len(sections), sections[:12]


def render_table_summary(tables: list[MasterTable]) -> list[str]:
    lines = [
        "| # | 한글명 | 코드 | 데이터 파일 | 필드수 | 스키마 폭(bytes) | 실제 파일 | 첫 레코드(bytes) | 레코드 수 | 비고 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for table in tables:
        path = DATA_DIR / table.filename
        if path.exists():
            line_count, sample_bytes, _, storage = summarize_text_file(path)
            note_parts: list[str] = []
            if storage != "plain":
                note_parts.append(storage)
            if sample_bytes and sample_bytes != table.schema_width:
                note_parts.append("폭 불일치")
            note = ", ".join(note_parts) if note_parts else ""
        else:
            line_count, sample_bytes = 0, 0
            note = "파일 없음"
        lines.append(
            f"| {table.index} | {table.name} | `{table.code}` | `{table.filename}` | {len(table.fields)} | {table.schema_width} | {'있음' if path.exists() else '없음'} | {sample_bytes} | {line_count} | {note or '-'} |"
        )
    return lines


def render_table_detail(table: MasterTable) -> list[str]:
    path = DATA_DIR / table.filename
    exists = path.exists()
    line_count, sample_bytes, sample, storage = summarize_text_file(path) if exists else (0, 0, "", "-")
    lines = [
        f"## {table.index}. `{table.filename}`",
        f"- 한글명: {table.name}",
        f"- 코드: `{table.code}`",
        f"- 실제 파일: {'있음' if exists else '없음'}",
        f"- 저장 형태: {storage}" if exists else "- 저장 형태: -",
        f"- 필드 수: {len(table.fields)}",
        f"- 스키마 폭: {table.schema_width} bytes",
        f"- 첫 레코드 폭: {sample_bytes} bytes" if exists else "- 첫 레코드 폭: -",
        f"- 레코드 수: {line_count}" if exists else "- 레코드 수: -",
    ]
    if exists and sample_bytes and sample_bytes != table.schema_width:
        lines.append("- 참고: 스키마 폭과 실제 첫 레코드 폭이 다릅니다. 겹치는 오프셋, 확장 필드, 또는 별도 포맷 차이 가능성이 있습니다.")
    if sample:
        lines.append(f"- 샘플: `{sample[:120]}`")
    lines.extend(
        [
            "",
            "| Index | 필드명 | FID | Offset | Length | Alias | Type |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for field in table.fields:
        alias = f"`{field.alias}`" if field.alias else "-"
        lines.append(
            f"| {field.index} | {field.label} | {field.fid} | {field.offset} | {field.length} | {alias} | {field.field_type} |"
        )
    lines.append("")
    return lines


def render_extra_files(known_files: set[str]) -> list[str]:
    lines = ["# 스키마 미등록 DAT", ""]
    extras = sorted(path for path in DATA_DIR.glob("*.dat") if path.name not in known_files)
    for path in extras:
        line_count, sample_bytes, sample, storage = summarize_text_file(path)
        lines.extend(
            [
                f"## `{path.name}`",
                f"- 저장 형태: {storage}",
                f"- 레코드 수: {line_count}",
                f"- 첫 레코드 폭: {sample_bytes} bytes",
                f"- 추정: {infer_extra_file(path.name, sample)}",
                f"- 샘플: `{sample[:120]}`" if sample else "- 샘플: -",
                "",
            ]
        )
    return lines


def render_conf_files() -> list[str]:
    appendix_count, appendix_sample = parse_mfeed_appendix(DATA_DIR / "xBusMFeedAppendix.conf")
    mapper_count, mapper_sample = parse_pc_mapper(DATA_DIR / "xBusPCMapper.conf")

    lines = [
        "# 설정 파일",
        "",
        "## `xBusMFeedAppendix.conf`",
        f"- 유효 항목 수: {appendix_count}",
        "- 성격: 영문 필드명 / FID / 자료형 / 한글 설명 사전",
        "- 샘플:",
        "```text",
        *appendix_sample,
        "```",
        "",
        "## `xBusPCMapper.conf`",
        f"- 섹션 수: {mapper_count}",
        "- 성격: 수신 메시지 코드별 필드 매핑 정의",
        "- 샘플 섹션:",
    ]
    for name, entry_count, sample_lines in mapper_sample:
        lines.append(f"- [{name}] 항목 {entry_count}개")
        for sample in sample_lines:
            lines.append(f"  - `{sample}`")
    lines.append("")
    return lines


def main() -> None:
    tables = parse_mastertable(DATA_DIR / "mastertable.fmt")
    known_files = {table.filename for table in tables}
    missing_files = sorted(name for name in known_files if not (DATA_DIR / name).exists())
    mismatch_codes: list[str] = []
    zip_wrapped: list[str] = []
    for table in tables:
        path = DATA_DIR / table.filename
        if not path.exists():
            continue
        _, sample_bytes, _, storage = summarize_text_file(path)
        if storage != "plain":
            zip_wrapped.append(f"`{table.filename}`")
        if sample_bytes and sample_bytes != table.schema_width:
            mismatch_codes.append(f"`{table.code}`")

    lines = [
        "# qry/data 분석",
        "",
        "## 요약",
        f"- `mastertable.fmt` 기준 스키마 테이블 수: {len(tables)}",
        f"- 실제 `.dat` 파일 수: {len(list(DATA_DIR.glob('*.dat')))}",
        f"- 실제 `.conf` 파일 수: {len(list(DATA_DIR.glob('*.conf')))}",
        "- `mastertable.fmt`는 `테이블명 / 코드 / 데이터 파일명 / 필드(FID, offset, length, alias)` 구조를 담고 있습니다.",
        "- 대부분의 `.dat`는 CRLF 줄바꿈을 가진 고정폭 텍스트 레코드로 보입니다.",
        f"- `mastertable.fmt`에 등록됐지만 디스크에 없는 파일: {', '.join(f'`{name}`' for name in missing_files) if missing_files else '-'}",
        f"- ZIP 래퍼로 저장된 예외: {', '.join(zip_wrapped) if zip_wrapped else '-'}",
        f"- 스키마 폭과 실제 첫 레코드 폭이 다른 코드: {', '.join(mismatch_codes) if mismatch_codes else '-'}",
        "",
        "## `mastertable.fmt` 해석",
        "- 파일 헤더의 첫 4바이트는 테이블 수이며 현재 값은 `42`입니다.",
        "- 각 테이블은 `필드수 -> 한글명 -> 코드 -> 데이터파일명 -> 필드반복` 순서로 저장됩니다.",
        "- 각 필드는 `타입(1 byte) / 한글 필드명 / FID(u32) / Offset(u32) / Length(u32) / Alias` 구조로 해석됩니다.",
        "",
        "## 스키마 요약",
        *render_table_summary(tables),
        "",
        "# 스키마 상세",
        "",
    ]

    for table in tables:
        lines.extend(render_table_detail(table))

    lines.extend(render_extra_files(known_files))
    lines.extend(render_conf_files())

    OUT_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
