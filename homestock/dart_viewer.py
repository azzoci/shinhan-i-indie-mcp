from __future__ import annotations

from dataclasses import dataclass
import html
import json
import re
from urllib.parse import urlencode
from urllib.request import urlopen


_DART_BASE_URL = "https://dart.fss.or.kr"
_MAIN_URL = f"{_DART_BASE_URL}/dsaf001/main.do"
_VIEWER_URL = f"{_DART_BASE_URL}/report/viewer.do"
SECTION_SELECTOR = "section[data-ele-id]"
SECTION_ID_ATTR = "data-ele-id"
SECTION_TITLE_ATTR = "data-title"
SECTION_TOC_ATTR = "data-toc-no"
PRINT_PAGE_BREAK_SELECTOR = "p.pgbrk, p.PGBRK"


@dataclass(frozen=True)
class DartViewerNode:
    rcp_no: str
    dcm_no: str
    ele_id: str
    offset: str
    length: str
    dtd: str
    text: str = ""
    toc_no: str = ""


@dataclass(frozen=True)
class DartViewerDocument:
    rcp_no: str
    content: str
    viewer_url: str
    dtd: str
    source: str = "dart_viewer"
    print_page_break_selector: str = ""


def disclosure_main_url(rcept_no: str) -> str:
    return f"{_MAIN_URL}?{urlencode({'rcpNo': rcept_no})}"


def disclosure_to_html(rcept_no: str) -> DartViewerDocument:
    """Return DART's officially rendered disclosure HTML for a receipt number."""
    main_html = _download_text(disclosure_main_url(rcept_no))
    nodes = parse_viewer_nodes(main_html)
    if not nodes:
        raise ValueError("DART viewer metadata could not be found")

    primary_dtd = nodes[0].dtd
    if len(nodes) == 1 and primary_dtd.upper() == "HTML":
        content = _ensure_base_href(_download_text(viewer_node_url(nodes[0])))
    else:
        fragments = [_viewer_body_fragment(_download_text(viewer_node_url(node))) for node in nodes]
        content = _combine_viewer_fragments(nodes, fragments)

    return DartViewerDocument(
        rcp_no=rcept_no,
        content=content,
        viewer_url=disclosure_main_url(rcept_no),
        dtd=primary_dtd,
        print_page_break_selector=_content_split_selector(content),
    )


def viewer_node_url(node: DartViewerNode) -> str:
    query = urlencode(
        {
            "rcpNo": node.rcp_no,
            "dcmNo": node.dcm_no,
            "eleId": node.ele_id,
            "offset": node.offset,
            "length": node.length,
            "dtd": node.dtd,
        }
    )
    return f"{_VIEWER_URL}?{query}"


def parse_viewer_nodes(main_html: str) -> list[DartViewerNode]:
    nodes = _parse_tree_nodes(main_html)
    if nodes:
        return nodes
    fallback = _parse_initial_view_doc(main_html)
    return [fallback] if fallback is not None else []


def looks_like_disclosure_body_html(raw_html: str, news_type: str | None = None) -> bool:
    # News type is routing metadata from the broker feed. It is not reliable
    # enough to prove that raw_html is already the disclosure body.
    del news_type

    content = str(raw_html or "").strip()
    if not content:
        return False

    lowered = content.lower()
    # News bodies frequently include disclosure links. Treat raw HTML as the
    # disclosure body only when it carries DART/KRX viewer markup, not merely
    # because the article type hints at disclosure or the page contains tables.
    disclosure_body_signals = (
        "xforms" in lowered,
        "report_xml.css" in lowered,
        _has_class(content, "pgbrk"),
        _has_disclosure_css_template(lowered),
    )
    if any(disclosure_body_signals):
        return True

    return False


def disclosure_content_split_selector(document_html: str) -> str:
    return _content_split_selector(document_html)


def _has_class(document_html: str, class_name: str) -> bool:
    pattern = rf"""(?is)<[^>]+\bclass\s*=\s*(['"])[^'"]*\b{re.escape(class_name)}\b[^'"]*\1"""
    return re.search(pattern, document_html) is not None


def _has_tag_class(document_html: str, tag_name: str, class_name: str) -> bool:
    pattern = rf"""(?is)<{re.escape(tag_name)}\b[^>]*\bclass\s*=\s*(['"])[^'"]*\b{re.escape(class_name)}\b[^'"]*\1"""
    return re.search(pattern, document_html) is not None


def _content_split_selector(document_html: str) -> str:
    if re.search(r"""(?is)<section\b[^>]*\bdata-ele-id\s*=""", document_html):
        return SECTION_SELECTOR
    if _has_tag_class(document_html, "p", "pgbrk"):
        return PRINT_PAGE_BREAK_SELECTOR
    return ""


def _has_disclosure_css_template(lowered_html: str) -> bool:
    css_classes = set(re.findall(r"\.([a-z][a-z0-9_-]*)\s*\{", lowered_html))
    if "pgbrk" in css_classes:
        return True
    dart_template_classes = {
        "cover-title",
        "section-1",
        "section-2",
        "section-3",
        "section-4",
        "part",
        "table-group",
        "correction",
    }
    return len(css_classes & dart_template_classes) >= 2


def _download_text(url: str) -> str:
    with urlopen(url, timeout=30) as response:
        payload = response.read()
        charset = response.headers.get_content_charset() or _detect_charset(payload) or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except LookupError:
        return payload.decode("utf-8", errors="replace")


def _detect_charset(payload: bytes) -> str | None:
    prefix = payload[:4096].decode("ascii", errors="ignore")
    match = re.search(r"charset\s*=\s*['\"]?([A-Za-z0-9._-]+)", prefix, re.IGNORECASE)
    return match.group(1) if match else None


def _parse_tree_nodes(main_html: str) -> list[DartViewerNode]:
    nodes: list[DartViewerNode] = []
    for match in re.finditer(r"(?is)var\s+node1\s*=\s*\{\s*\};(?P<body>.*?)treeData\.push\(node1\)", main_html):
        values = _parse_node_assignments(match.group("body"))
        node = _node_from_values(values)
        if node is not None:
            nodes.append(node)
    return nodes


def _parse_node_assignments(block: str) -> dict[str, str]:
    values: dict[str, str] = {}
    pattern = re.compile(
        r"""node1\[['"](?P<key>[^'"]+)['"]\]\s*=\s*(?P<value>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^;]+)\s*;""",
        re.VERBOSE | re.DOTALL,
    )
    for match in pattern.finditer(block):
        values[match.group("key")] = _decode_js_value(match.group("value"))
    return values


def _parse_initial_view_doc(main_html: str) -> DartViewerNode | None:
    match = re.search(
        r'viewDoc\(\s*"(?P<rcp>[^"]*)"\s*,\s*"(?P<dcm>[^"]*)"\s*,\s*"(?P<ele>[^"]*)"\s*,\s*"(?P<offset>[^"]*)"\s*,\s*"(?P<length>[^"]*)"\s*,\s*"(?P<dtd>[^"]*)"',
        main_html,
        re.VERBOSE | re.DOTALL,
    )
    if match is None:
        return None
    return DartViewerNode(
        rcp_no=html.unescape(match.group("rcp")),
        dcm_no=html.unescape(match.group("dcm")),
        ele_id=html.unescape(match.group("ele")),
        offset=html.unescape(match.group("offset")),
        length=html.unescape(match.group("length")),
        dtd=html.unescape(match.group("dtd")),
    )


def _node_from_values(values: dict[str, str]) -> DartViewerNode | None:
    required = ["rcpNo", "dcmNo", "eleId", "offset", "length", "dtd"]
    if any(not values.get(key) for key in required):
        return None
    return DartViewerNode(
        rcp_no=values["rcpNo"],
        dcm_no=values["dcmNo"],
        ele_id=values["eleId"],
        offset=values["offset"],
        length=values["length"],
        dtd=values["dtd"],
        text=values.get("text", ""),
        toc_no=values.get("tocNo", ""),
    )


def _decode_js_value(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if value[0] in {"'", '"'} and value[-1:] == value[0]:
        if value[0] == '"':
            try:
                return html.unescape(str(json.loads(value)))
            except json.JSONDecodeError:
                pass
        return html.unescape(value[1:-1].replace(r"\'", "'").replace(r"\"", '"').replace(r"\\", "\\"))
    return html.unescape(value.strip())


def _viewer_body_fragment(viewer_html: str) -> str:
    match = re.search(r"(?is)<body\b[^>]*>(?P<body>.*?)</body\s*>", viewer_html)
    return match.group("body").strip() if match is not None else viewer_html.strip()


def _combine_viewer_fragments(nodes: list[DartViewerNode], fragments: list[str]) -> str:
    sections: list[str] = []
    for node, fragment in zip(nodes, fragments):
        title = html.escape(node.text, quote=True)
        attrs = (
            f'{SECTION_ID_ATTR}="{html.escape(node.ele_id, quote=True)}" '
            f'{SECTION_TOC_ATTR}="{html.escape(node.toc_no, quote=True)}" '
            f'{SECTION_TITLE_ATTR}="{title}"'
        )
        sections.append(f"<section {attrs}>\n{fragment}\n</section>")

    return "\n".join(
        [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            '<meta charset="utf-8">',
            f'<base href="{_DART_BASE_URL}/">',
            '<link rel="stylesheet" type="text/css" href="/css/report_xml.css">',
            "</head>",
            "<body>",
            *sections,
            "</body>",
            "</html>",
            "",
        ]
    )


def _ensure_base_href(document_html: str) -> str:
    if re.search(r"(?is)<base\b", document_html):
        return document_html
    base_tag = f'<base href="{_DART_BASE_URL}/">'
    if re.search(r"(?is)<head\b[^>]*>", document_html):
        return re.sub(r"(?is)(<head\b[^>]*>)", rf"\1{base_tag}", document_html, count=1)
    if re.search(r"(?is)<html\b[^>]*>", document_html):
        return re.sub(r"(?is)(<html\b[^>]*>)", rf"\1<head>{base_tag}</head>", document_html, count=1)
    return f"<!DOCTYPE html><html><head>{base_tag}</head><body>{document_html}</body></html>"
