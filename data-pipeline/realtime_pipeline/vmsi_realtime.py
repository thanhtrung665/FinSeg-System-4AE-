# -*- coding: utf-8 -*-
"""
realtime_pipeline/vmsi_realtime.py

Engine tinh VMSI real-time cho 1 ma co phieu bat ky.
Ket noi tat ca nguon du lieu thuc → tinh VMSI → ghi live_vmsi.json.

Su dung: chi can goi RealtimeVMSIEngine(ticker).run_cycle()
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

import sys
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PIPELINE_ROOT))


class RealtimeVMSIEngine:
    """
    Engine tinh VMSI tu du lieu thuc thu thap truc tiep (khong qua Kafka/demo CSV).

    Quy trinh moi chu ky:
      1. Crawl Facebook posts + News articles lien quan den ticker
      2. Crawl NHNN van ban → ingest ChromaDB
      3. Lay gia co phieu tu vnstock
      4. Chuan hoa → tinh phobert_score (keyword-based)
      5. Goi MACSystem.execute_sequential_workflow() (dung agent hien co)
      6. Enrich ket qua voi market_sentiment tu gia co phieu
      7. Ghi live_vmsi.json
    """

    def __init__(self, ticker: str = "SHB"):
        self.ticker  = ticker.upper()
        self.logger  = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"RealtimeVMSIEngine khoi tao cho ticker: {self.ticker}")

        # Import lazy de tranh loi khi module chua san sang
        self._producer   = None
        self._mac        = None
        self._ingest_done = False   # Danh dau da ingest ChromaDB lan dau chua

    # ── Lazy init ─────────────────────────────────────────────────────────────

    def _get_producer(self):
        if self._producer is None:
            from realtime_pipeline.producers.realtime_producer import RealtimeProducer
            self._producer = RealtimeProducer()
        return self._producer

    def _get_mac(self):
        if self._mac is None:
            from multi_agent_system.agents.mac_orchestrator import MACSystem
            self._mac = MACSystem()
        return self._mac

    # ── Step 1: Thu thap Social ────────────────────────────────────────────────

    def _collect_social(self) -> int:
        """
        Thu thap Facebook posts + News:
        - Facebook posts  → Kafka fb_mock_data (SocialAgent xu ly sentiment)
        - News articles   → Kafka fb_mock_data + ChromaDB (MacroAgent RAG query)
        """
        from realtime_pipeline.crawlers.facebook_crawler import crawl_facebook_for_ticker
        from realtime_pipeline.crawlers.news_crawler     import crawl_news_for_ticker
        from realtime_pipeline.normalizers.unified_normalizer import (
            normalize_social_batch, normalize_news_batch, normalize_news_article
        )

        total_pushed = 0

        # Facebook posts → Kafka (sentiment scoring)
        try:
            posts = crawl_facebook_for_ticker(self.ticker)
            if posts:
                normalized = normalize_social_batch(posts, self.ticker)
                pushed = self._get_producer().push_social(normalized)
                total_pushed += pushed
                self.logger.info(f"[Social] {pushed} Facebook posts → Kafka")
        except Exception as e:
            self.logger.error(f"Loi crawl Facebook: {e}")

        # News articles:
        #   1. → Kafka (SocialAgent doc de tinh S_social)
        #   2. → ChromaDB (MacroAgent RAG query tin tuc moi nhat)
        try:
            articles = crawl_news_for_ticker(self.ticker)
            if articles:
                # Push vao Kafka
                normalized_kafka = normalize_news_batch(articles, self.ticker)
                pushed = self._get_producer().push_social(normalized_kafka)
                total_pushed += pushed
                self.logger.info(f"[Social] {pushed} news articles → Kafka")

                # DONG THOI: embed va ingest vao ChromaDB de MacroAgent RAG
                # Chi ingest cac bai co content du chat luong (>= 100 ky tu)
                chroma_docs = []
                for art in articles:
                    if len(art.content_text) >= 100:
                        n = normalize_news_article(art, self.ticker)
                        # Them truong metadata cho ChromaDB
                        n["metadata"] = {
                            "source_file":    n.get("article_id", ""),
                            "ticker_context": self.ticker,
                            "publish_date":   n.get("published_at", ""),
                            "document_type":  "news_article",
                            "source":         n.get("source", ""),
                            "url":            n.get("url", ""),
                        }
                        chroma_docs.append(n)

                if chroma_docs:
                    # Push vao Kafka policy_data → Vector Worker se xu ly embedding + ingest ChromaDB
                    ingested = self._get_producer().push_policies(chroma_docs)
                    self.logger.info(
                        f"[Social] {ingested}/{len(chroma_docs)} news articles → Kafka policy_data (Vector Worker xu ly)"
                    )
        except Exception as e:
            self.logger.error(f"Loi crawl news: {e}")

        return total_pushed

    # ── Step 2: Thu thap + Ingest chinh sach NHNN ─────────────────────────────

    def _collect_and_ingest_policies(self) -> int:
        """
        Crawl NHNN → ingest vao ChromaDB realtime collection.
        Chi chay full ingest lan dau hoac moi 6 chu ky (3 gio).
        """
        from realtime_pipeline.crawlers.nhnn_crawler     import crawl_nhnn_all_types
        from realtime_pipeline.normalizers.unified_normalizer import normalize_policy_batch

        try:
            docs = crawl_nhnn_all_types()
            if docs:
                normalized = normalize_policy_batch(docs, self.ticker)
                # Push vao Kafka policy_data → Vector Worker se xu ly embedding + ingest ChromaDB
                ingested = self._get_producer().push_policies(normalized)
                self.logger.info(f"[Policy] {ingested} van ban NHNN → Kafka policy_data (Vector Worker xu ly)")
                return ingested
        except Exception as e:
            self.logger.error(f"Loi ingest NHNN: {e}")
        return 0

    # ── Step 3: Thu thap gia co phieu ─────────────────────────────────────────

    def _collect_market(self) -> float:
        """
        Lay gia co phieu → day vao Kafka market_stock_data.
        Tra ve market_sentiment [-1, 1] de dung trong tinh VMSI.
        """
        from realtime_pipeline.crawlers.stock_crawler import (
            crawl_stocks_for_ticker
        )
        from realtime_pipeline.normalizers.unified_normalizer import normalize_stock_batch

        try:
            result = crawl_stocks_for_ticker(self.ticker)
            bars   = result.get("historical_bars", [])

            if bars:
                normalized = normalize_stock_batch(bars, self.ticker)
                pushed = self._get_producer().push_market(normalized)
                self.logger.info(f"[Market] {pushed} bars → Kafka")

            # Them realtime bar neu co
            rt_bar = result.get("realtime_bar")
            if rt_bar:
                from realtime_pipeline.normalizers.unified_normalizer import normalize_stock_bar
                self._get_producer().push_market([normalize_stock_bar(rt_bar, self.ticker)])

            mkt_sentiment = result.get("market_sentiment", 0.0)
            self.logger.info(f"[Market] sentiment = {mkt_sentiment:+.4f}")
            return float(mkt_sentiment)

        except Exception as e:
            self.logger.error(f"Loi crawl stock: {e}")
            return 0.0

    # ── Step 4: Chay MAC System ────────────────────────────────────────────────

    def _run_mac_cycle(self) -> dict:
        """Chay MACSystem.execute_sequential_workflow() cho ticker hien tai."""
        try:
            mac    = self._get_mac()
            result = mac.execute_sequential_workflow(ticker_context=self.ticker)
            return result
        except Exception as e:
            self.logger.error(f"Loi MAC cycle: {e}")
            return {"error": str(e)}

    # ── Step 5: Enrich voi market sentiment ────────────────────────────────────

    def _enrich_with_market(self, vmsi_result: dict, market_sentiment: float) -> dict:
        """
        Neu MAC co s_news = 0 (hien tai luon la 0), thay bang market_sentiment.
        Dieu nay giup VMSI phan anh thuc te gia co phieu.
        """
        if "error" in vmsi_result or not vmsi_result:
            return vmsi_result

        # Lay lai cac component scores
        scores = vmsi_result.get("component_scores", {})
        s_nhnn = scores.get("s_nhnn", 0)
        s_social = scores.get("s_social", 0.0)

        # Dung market_sentiment lam s_news
        s_news = market_sentiment

        # Tinh lai VMSI voi s_news thuc
        try:
            from multi_agent_system.engines.vmsi_engine import VMSIEngine
            engine   = VMSIEngine()
            s_macro  = engine.calculate_macro_score(s_nhnn, s_news)
            i_raw    = engine.calculate_raw_index(s_macro, s_social)
            vmsi_raw = engine.calculate_final_vmsi(i_raw)

            # EMA voi gia tri cu
            prev_vmsi = vmsi_result.get("vmsi_value", 50.0)
            vmsi_ema  = engine.apply_ema_smoothing(vmsi_raw, prev_vmsi)

            # Cap nhat payload
            vmsi_result["vmsi_value"] = round(vmsi_ema, 2)
            vmsi_result["component_scores"]["s_news"]    = round(s_news, 4)
            vmsi_result["component_scores"]["s_macro"]   = round(s_macro, 4)
            vmsi_result["component_scores"]["vmsi_raw"]  = round(vmsi_raw, 2)
            vmsi_result["realtime_enriched"] = True
            vmsi_result["market_sentiment"]  = round(market_sentiment, 4)
            vmsi_result["ticker"]            = self.ticker

        except Exception as e:
            self.logger.warning(f"Loi enrich VMSI voi market: {e}")

        return vmsi_result

    # ── Public API ────────────────────────────────────────────────────────────

    def run_cycle(self, ingest_policies: bool = True) -> dict:
        """
        Chay 1 chu ky phan tich VMSI day du:
          1. Thu thap social + news → Kafka
          2. (Tuy chon) Ingest NHNN → ChromaDB
          3. Thu thap gia co phieu → Kafka + lay market_sentiment
          4. Chay MACSystem (Social → Macro → Risk)
          5. Enrich voi market_sentiment
          6. Tra ve ket qua VMSI

        Ket qua duoc ghi tu dong vao live_vmsi.json boi RiskSynthesisAgent.
        """
        t0 = time.time()
        self.logger.info(f"=== BAT DAU CHU KY REALTIME [{self.ticker}] ===")

        # Step 1: Social
        social_count = self._collect_social()
        self.logger.info(f"[1/4] Social: {social_count} messages → Kafka")

        # Step 2: NHNN policies (lan dau hoac theo policy_refresh_flag)
        if ingest_policies:
            self._collect_and_ingest_policies()
            self.logger.info("[2/4] NHNN policies → ChromaDB")
        else:
            self.logger.info("[2/4] Bo qua ingest NHNN (da co data)")

        # Step 3: Market
        market_sentiment = self._collect_market()
        self.logger.info(f"[3/4] Market sentiment: {market_sentiment:+.4f}")

        # Step 4: MAC cycle
        self.logger.info("[4/4] Chay MAC System...")
        vmsi_result = self._run_mac_cycle()

        # Step 5: Enrich
        vmsi_result = self._enrich_with_market(vmsi_result, market_sentiment)

        elapsed = time.time() - t0
        self.logger.info(
            f"=== HOAN THANH CHU KY [{self.ticker}] "
            f"| VMSI={vmsi_result.get('vmsi_value', 'ERR')} "
            f"| {elapsed:.2f}s ==="
        )
        return vmsi_result

    def shutdown(self):
        """Dong tat ca ket noi."""
        if self._producer:
            self._producer.close()
        if self._mac:
            try:
                self._mac.shutdown()
            except Exception:
                pass
        self.logger.info("RealtimeVMSIEngine shutdown.")
