from __future__ import annotations

import io
import re
import zipfile
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET


_DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"
_CELL_TAGS = {"TD", "TH", "TE", "TU"}


def disclosure_to_markdown(rcept_no: str, api_key: str) -> str:
    """Download a DART disclosure XML document and convert it to raw markdown."""
    xml_bytes = _download_disclosure_xml(rcept_no, api_key)
    return disclosure_xml_to_markdown(xml_bytes, rcept_no=rcept_no)


def disclosure_xml_to_markdown(xml_bytes: bytes, rcept_no: str | None = None) -> str:
    """Convert a DART disclosure XML payload into raw markdown."""
    root = ET.fromstring(xml_bytes)
    document_name = _clean(root.findtext("DOCUMENT-NAME", default=""))
    company_name = _clean(root.findtext("COMPANY-NAME", default=""))
    formula_version = _clean(root.findtext("FORMULA-VERSION", default=""))

    lines: list[str] = []
    if document_name:
        lines.append(f"# {document_name}")
        lines.append("")
    if company_name:
        lines.append(f"- 회사명: {company_name}")
    if formula_version:
        lines.append(f"- 수식 버전: {formula_version}")
    if rcept_no:
        lines.append(f"- 공시번호: {rcept_no}")
    if company_name or formula_version or rcept_no:
        lines.append("")

    body = root.find("BODY")
    if body is not None:
        _render_children(body, lines, parent_tag=body.tag)

    markdown = "\n".join(lines)
    markdown = re.sub(r"\n{3,}", "\n\n", markdown).strip()
    return f"{markdown}\n"


def _download_disclosure_xml(rcept_no: str, api_key: str) -> bytes:
    query = urlencode({"crtfc_key": api_key, "rcept_no": rcept_no})
    with urlopen(f"{_DOCUMENT_URL}?{query}", timeout=30) as response:
        payload = response.read()

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        xml_names = sorted(name for name in archive.namelist() if name.lower().endswith(".xml"))
        if not xml_names:
            raise ValueError("OpenDART response did not contain an XML document")
        return archive.read(xml_names[0])


def _render_children(elem: ET.Element, lines: list[str], parent_tag: str) -> None:
    children = list(elem)
    skip_next_p = False
    for child in children:
        if skip_next_p and child.tag == "P":
            skip_next_p = False
            continue
        skip_next_p = False
        tag = child.tag
        if tag in {"COVER", "SECTION-1", "SECTION-2", "SECTION-3", "SECTION-4", "SECTION-5", "SECTION-6"}:
            _render_children(child, lines, parent_tag=tag)
            continue
        if tag == "COVER-TITLE":
            title = _gather_text(child)
            if title:
                lines.append(f"# {title}")
                lines.append("")
            continue
        if tag == "TITLE":
            title = _normalize_label(_gather_text(child))
            if not title:
                continue
            single_value = _inline_value_from_title(child, elem)
            if single_value is not None:
                lines.append(f"{title} {single_value}")
                lines.append("")
                skip_next_p = True
            else:
                lines.append(f"{_heading_prefix(parent_tag)} {title}")
                lines.append("")
            continue
        if tag == "P":
            text = _gather_text(child)
            if text:
                lines.append(text)
                lines.append("")
            continue
        if tag == "TABLE":
            table_lines = _render_table(child)
            if table_lines:
                lines.extend(table_lines)
                lines.append("")
            continue
        if tag == "TABLE-GROUP":
            group_lines = _render_table_group(child)
            if group_lines:
                lines.extend(group_lines)
                lines.append("")
            continue
        if tag == "PGBRK":
            lines.append("---")
            lines.append("")
            continue
        _render_unknown(child, lines)


def _inline_value_from_title(title_elem: ET.Element, parent: ET.Element) -> str | None:
    siblings = list(parent)
    try:
        index = siblings.index(title_elem)
    except ValueError:
        return None
    if index + 1 >= len(siblings):
        return None
    next_elem = siblings[index + 1]
    if next_elem.tag != "P":
        return None
    if _clean("".join(next_elem.itertext())) == "":
        return None
    if index + 2 < len(siblings) and siblings[index + 2].tag == "P":
        return None
    value = _gather_text(next_elem)
    return value or None


def _render_table_group(group: ET.Element) -> list[str]:
    rendered: list[str] = []
    for child in list(group):
        if child.tag == "TABLE":
            table_lines = _render_table(child)
            if table_lines:
                if rendered:
                    rendered.append("")
                rendered.extend(table_lines)
        elif child.tag == "P":
            text = _gather_text(child)
            if text:
                if rendered:
                    rendered.append("")
                rendered.append(text)
        else:
            fallback = _render_unknown_block(child)
            if fallback:
                if rendered:
                    rendered.append("")
                rendered.extend(fallback)
    return rendered


def _render_table(table: ET.Element) -> list[str]:
    rows: list[list[str]] = []
    header_rows = 0
    for section in list(table):
        if section.tag not in {"THEAD", "TBODY"}:
            continue
        section_rows = _render_table_section(section)
        for row in section_rows:
            if any(cell.strip() for cell in row):
                rows.append(row)
                if section.tag == "THEAD":
                    header_rows += 1
    if not rows:
        return []

    width = max(len(row) for row in rows)
    padded_rows = [row + [" "] * (width - len(row)) for row in rows]
    if header_rows == 0:
        header_rows = 1
    header = padded_rows[:header_rows]
    body = padded_rows[header_rows:]

    if len(header) == 1:
        header_line = header[0]
    else:
        header_line = _flatten_header_rows(header)

    rendered = [
        "| " + " | ".join(header_line) + " |",
        "| " + " | ".join(["---"] * width) + " |",
    ]
    for row in body:
        rendered.append("| " + " | ".join(row) + " |")
    return rendered


def _render_table_section(section: ET.Element) -> list[list[str]]:
    rows: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}
    for tr in section.findall("TR"):
        row: list[str] = []
        current_pending = pending
        next_pending: dict[int, tuple[str, int]] = {}
        col = 0

        def flush_until(target_col: int) -> tuple[list[str], dict[int, tuple[str, int]], int]:
            nonlocal row, next_pending, col
            while col < target_col:
                if col in current_pending:
                    text, remaining = current_pending[col]
                    row.append(text)
                    if remaining > 1:
                        next_pending[col] = (text, remaining - 1)
                else:
                    row.append(" ")
                col += 1
            return row, next_pending, col

        for cell in list(tr):
            if cell.tag not in _CELL_TAGS:
                continue
            while col in current_pending:
                flush_until(col + 1)
            text = _gather_text(cell) or " "
            colspan = _safe_int(cell.attrib.get("COLSPAN"), default=1)
            rowspan = _safe_int(cell.attrib.get("ROWSPAN"), default=1)
            for offset in range(colspan):
                propagated = text
                row.append(propagated)
                if rowspan > 1:
                    next_pending[col + offset] = (propagated, rowspan - 1)
                col += 1

        if current_pending:
            flush_until(max(col, max(current_pending) + 1))

        rows.append(row)
        pending = next_pending
    return rows


def _render_unknown(elem: ET.Element, lines: list[str]) -> None:
    block = _render_unknown_block(elem)
    if not block:
        return
    lines.extend(block)
    lines.append("")


def _render_unknown_block(elem: ET.Element) -> list[str]:
    attrs = " ".join(f'{key}="{value}"' for key, value in sorted(elem.attrib.items()))
    open_tag = f"<{elem.tag}{(' ' + attrs) if attrs else ''}>"
    close_tag = f"</{elem.tag}>"
    content = _gather_text(elem)
    if not content:
        return [open_tag, close_tag]
    return [open_tag, content, close_tag]


def _flatten_header_rows(header_rows: list[list[str]]) -> list[str]:
    flattened: list[str] = []
    for column in zip(*header_rows):
        values: list[str] = []
        for value in column:
            cleaned = _clean(value)
            if not cleaned or cleaned in values:
                continue
            values.append(cleaned)
        flattened.append(_compose_header(values))
    return flattened


def _compose_header(values: list[str]) -> str:
    if not values:
        return " "
    if len(values) == 1:
        return values[0]
    return " > ".join(values)


def _heading_prefix(parent_tag: str) -> str:
    if parent_tag == "SECTION-3":
        return "###"
    if parent_tag == "SECTION-4":
        return "####"
    if parent_tag == "SECTION-5":
        return "#####"
    return "##"


def _normalize_label(text: str) -> str:
    normalized = _clean(text)
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\s+:", ":", normalized)
    if normalized.endswith(":"):
        head = normalized[:-1]
        return _compact_spaced_syllables(head) + ":"
    return normalized


def _compact_spaced_syllables(text: str) -> str:
    tokens = text.split(" ")
    compacted: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if len(token) == 1:
            group = [token]
            index += 1
            while index < len(tokens) and len(tokens[index]) == 1:
                group.append(tokens[index])
                index += 1
            compacted.append("".join(group))
            continue
        compacted.append(token)
        index += 1
    return " ".join(part for part in compacted if part)


def _gather_text(elem: ET.Element) -> str:
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in list(elem):
        parts.extend(_iter_child_text(child))
        if child.tail:
            parts.append(child.tail)
    return _clean(" ".join(part for part in parts if part))


def _iter_child_text(elem: ET.Element) -> Iterable[str]:
    if elem.tag == "BR":
        yield "\n"
    elif elem.tag == "SUP":
        text = _gather_text(elem)
        if text:
            yield f"^{text}"
    elif elem.tag == "SUB":
        text = _gather_text(elem)
        if text:
            yield f"_{text}"
    else:
        if elem.text:
            yield elem.text
        for child in list(elem):
            yield from _iter_child_text(child)
            if child.tail:
                yield child.tail


def _clean(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def _safe_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default
