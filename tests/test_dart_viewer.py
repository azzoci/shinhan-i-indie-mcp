from __future__ import annotations

import unittest
from unittest.mock import patch

from homestock.dart_viewer import (
    PRINT_PAGE_BREAK_SELECTOR,
    SECTION_SELECTOR,
    disclosure_to_html,
    looks_like_disclosure_body_html,
    parse_viewer_nodes,
)


class DartViewerTests(unittest.TestCase):
    def test_parse_viewer_nodes_reads_xml_tree_metadata(self):
        main_html = """
        var node1 = {};
        node1['text'] = "자기주식취득결과보고서";
        node1['id'] = "1";
        node1['rcpNo'] = "20260424000778";
        node1['dcmNo'] = "11346807";
        node1['eleId'] = "1";
        node1['offset'] = "573";
        node1['length'] = "2390";
        node1['dtd'] = "dart4.xsd";
        node1['tocNo'] =  "1";
        treeData.push(node1);
        """

        nodes = parse_viewer_nodes(main_html)

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].rcp_no, "20260424000778")
        self.assertEqual(nodes[0].dcm_no, "11346807")
        self.assertEqual(nodes[0].ele_id, "1")
        self.assertEqual(nodes[0].offset, "573")
        self.assertEqual(nodes[0].length, "2390")
        self.assertEqual(nodes[0].dtd, "dart4.xsd")
        self.assertEqual(nodes[0].text, "자기주식취득결과보고서")

    def test_parse_viewer_nodes_falls_back_to_initial_html_viewdoc(self):
        main_html = 'viewDoc("20260511800596", "11371626", "0", "0", "0", "HTML", "");'

        nodes = parse_viewer_nodes(main_html)

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].rcp_no, "20260511800596")
        self.assertEqual(nodes[0].dtd, "HTML")

    def test_disclosure_to_html_combines_xml_viewer_fragments(self):
        main_html = """
        var node1 = {};
        node1['text'] = "표지";
        node1['rcpNo'] = "20260424000778";
        node1['dcmNo'] = "11346807";
        node1['eleId'] = "1";
        node1['offset'] = "573";
        node1['length'] = "2390";
        node1['dtd'] = "dart4.xsd";
        node1['tocNo'] =  "1";
        treeData.push(node1);
        var node1 = {};
        node1['text'] = "1. 발행인의 명칭 및 주소";
        node1['rcpNo'] = "20260424000778";
        node1['dcmNo'] = "11346807";
        node1['eleId'] = "2";
        node1['offset'] = "3022";
        node1['length'] = "419";
        node1['dtd'] = "dart4.xsd";
        node1['tocNo'] =  "2";
        treeData.push(node1);
        """
        responses = {
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260424000778": main_html,
            "https://dart.fss.or.kr/report/viewer.do?rcpNo=20260424000778&dcmNo=11346807&eleId=1&offset=573&length=2390&dtd=dart4.xsd": (
                "<html><body><p>표지 본문</p><p class='pgbrk'></p></body></html>"
            ),
            "https://dart.fss.or.kr/report/viewer.do?rcpNo=20260424000778&dcmNo=11346807&eleId=2&offset=3022&length=419&dtd=dart4.xsd": (
                "<html><body><p>주소 본문</p></body></html>"
            ),
        }

        with patch("homestock.dart_viewer._download_text", side_effect=lambda url: responses[url]):
            document = disclosure_to_html("20260424000778")

        self.assertEqual(document.dtd, "dart4.xsd")
        self.assertEqual(document.viewer_url, "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260424000778")
        self.assertEqual(document.print_page_break_selector, SECTION_SELECTOR)
        self.assertIn("표지 본문", document.content)
        self.assertIn("주소 본문", document.content)
        self.assertIn("class='pgbrk'", document.content)
        self.assertIn('data-ele-id="1"', document.content)
        self.assertIn('data-ele-id="2"', document.content)

    def test_disclosure_to_html_returns_single_html_viewer_document(self):
        main_html = 'viewDoc("20260511800596", "11371626", "0", "0", "0", "HTML", "");'
        viewer_html = "<html><head></head><body><div class='xforms'>본문</div></body></html>"
        responses = {
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260511800596": main_html,
            "https://dart.fss.or.kr/report/viewer.do?rcpNo=20260511800596&dcmNo=11371626&eleId=0&offset=0&length=0&dtd=HTML": viewer_html,
        }

        with patch("homestock.dart_viewer._download_text", side_effect=lambda url: responses[url]):
            document = disclosure_to_html("20260511800596")

        self.assertEqual(document.dtd, "HTML")
        self.assertEqual(document.print_page_break_selector, "")
        self.assertIn("<base href=\"https://dart.fss.or.kr/\">", document.content)
        self.assertIn("본문", document.content)

    def test_disclosure_to_html_returns_print_page_break_selector_when_no_sections_exist(self):
        main_html = 'viewDoc("20260511800596", "11371626", "0", "0", "0", "HTML", "");'
        viewer_html = "<html><head></head><body><p>본문</p><P class='pgbrk'></P></body></html>"
        responses = {
            "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260511800596": main_html,
            "https://dart.fss.or.kr/report/viewer.do?rcpNo=20260511800596&dcmNo=11371626&eleId=0&offset=0&length=0&dtd=HTML": viewer_html,
        }

        with patch("homestock.dart_viewer._download_text", side_effect=lambda url: responses[url]):
            document = disclosure_to_html("20260511800596")

        self.assertEqual(document.print_page_break_selector, PRINT_PAGE_BREAK_SELECTOR)

    def test_looks_like_disclosure_body_html_ignores_news_type_without_body_signals(self):
        self.assertFalse(looks_like_disclosure_body_html("<table>" + ("x" * 500) + "</table>", news_type="S"))
        self.assertFalse(
            looks_like_disclosure_body_html(
                "<html><body><table><tr><td>뉴스 표</td></tr></table>"
                '<a href="/dsaf001/main.do?rcpNo=20260511800596">공시</a></body></html>',
                news_type="S",
            )
        )
        self.assertFalse(looks_like_disclosure_body_html("<p>일반 뉴스</p>", news_type="S"))
        self.assertFalse(looks_like_disclosure_body_html("<style>.page-break {page-break-after:always}</style>"))
        self.assertTrue(looks_like_disclosure_body_html("<style>.PGBRK {page-break-after:always}</style>"))
        self.assertTrue(looks_like_disclosure_body_html("<style>.COVER-TITLE{} .SECTION-1{}</style>"))
        self.assertFalse(looks_like_disclosure_body_html("<style>.SECTION-1{}</style>"))
        self.assertTrue(looks_like_disclosure_body_html("<div class='xforms'><table></table></div>"))
        self.assertTrue(looks_like_disclosure_body_html("<html><body><p class='pgbrk'></p></body></html>"))
        self.assertTrue(looks_like_disclosure_body_html("<html><body><p class='PGBRK'></p></body></html>"))


if __name__ == "__main__":
    unittest.main()
