# -*- coding: utf-8 -*-
"""
realtime_pipeline/scheduler.py

Scheduler chay chu ky phan tich VMSI tu dong moi 30 phut.
Su dung APScheduler (BackgroundScheduler) de khong block main thread.

Su dung:
  # Chay truc tiep:
  python realtime_pipeline/scheduler.py --ticker SHB

  # Import vao code khac:
  from realtime_pipeline.scheduler import RealtimeScheduler
  sched = RealtimeScheduler(ticker="SHB", interval_seconds=1800)
  sched.start()
"""

import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import Optional

# ── Setup path ────────────────────────────────────────────────────────────────
_PIPELINE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PIPELINE_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("RealtimeScheduler")


class RealtimeScheduler:
    """
    Quan ly vong lap phan tich VMSI theo chu ky.

    - interval_seconds: Mac dinh 1800s (30 phut)
    - Chu ky dau tien: chay ngay lap tuc
    - ingest_policies_every: Ingest NHNN sau bao nhieu chu ky (default 6 = 3 gio)
    """

    def __init__(
        self,
        ticker:                    str = "SHB",
        interval_seconds:          int = 1800,
        ingest_policies_every:     int = 6,
    ):
        self.ticker                = ticker.upper()
        self.interval_seconds      = interval_seconds
        self.ingest_policies_every = ingest_policies_every
        self._cycle_count          = 0
        self._engine               = None
        self._running              = False

        logger.info(
            f"Scheduler khoi tao | ticker={self.ticker} "
            f"| interval={interval_seconds}s "
            f"| ingest_policy_every={ingest_policies_every} chu ky"
        )

    def _get_engine(self):
        if self._engine is None:
            from realtime_pipeline.vmsi_realtime import RealtimeVMSIEngine
            self._engine = RealtimeVMSIEngine(ticker=self.ticker)
        return self._engine

    def _run_single_cycle(self):
        """Chay 1 chu ky VMSI."""
        self._cycle_count += 1
        should_ingest = (self._cycle_count % self.ingest_policies_every == 1)

        logger.info(
            f"--- Chu ky #{self._cycle_count} | ticker={self.ticker} "
            f"| ingest_nhnn={'YES' if should_ingest else 'NO'} ---"
        )

        try:
            engine = self._get_engine()
            result = engine.run_cycle(ingest_policies=should_ingest)

            vmsi   = result.get("vmsi_value", "N/A")
            status = result.get("status",     "N/A")
            logger.info(
                f"Chu ky #{self._cycle_count} HOAN THANH | "
                f"VMSI={vmsi} | Status={status}"
            )
            return result
        except Exception as e:
            logger.error(f"Chu ky #{self._cycle_count} LOI: {e}")
            return {"error": str(e)}

    def start_blocking(self):
        """
        Chay vong lap blocking (goi tu main thread).
        Dung Ctrl+C de dung.
        """
        self._running = True
        logger.info(
            f"Bat dau chay realtime pipeline | "
            f"ticker={self.ticker} | chu ky={self.interval_seconds}s"
        )

        def _shutdown(sig, frame):
            logger.info("Nhan tin hieu dung (SIGINT/SIGTERM)...")
            self._running = False

        signal.signal(signal.SIGINT,  _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        while self._running:
            self._run_single_cycle()
            if self._running:
                logger.info(
                    f"Chu ky tiep theo sau {self.interval_seconds}s "
                    f"({self.interval_seconds // 60} phut)..."
                )
                # Dem nguoc de co the dung ngay lap tuc
                for _ in range(self.interval_seconds):
                    if not self._running:
                        break
                    time.sleep(1)

        # Shutdown
        if self._engine:
            self._engine.shutdown()
        logger.info("Realtime scheduler da dung.")

    def start_background(self):
        """
        Chay vong lap trong background thread (non-blocking).
        Dung voi APScheduler hoac Streamlit.
        Tra ve scheduler object de caller co the stop().
        """
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.interval     import IntervalTrigger

            sched = BackgroundScheduler(
                timezone="Asia/Ho_Chi_Minh",
                job_defaults={"coalesce": True, "max_instances": 1},
            )

            # Chay ngay lan dau (delay 2s de he thong khoi dong xong)
            sched.add_job(
                self._run_single_cycle,
                trigger=IntervalTrigger(seconds=self.interval_seconds),
                id=f"vmsi_{self.ticker}",
                replace_existing=True,
                next_run_time=None,   # se set ngay sau khi start
            )

            sched.start()

            # Chay ngay lap tuc chu ky dau
            import threading
            t = threading.Thread(
                target=self._run_single_cycle, daemon=True, name="vmsi_first_cycle"
            )
            t.start()

            logger.info(
                f"Background scheduler dang chay | "
                f"ticker={self.ticker} | moi {self.interval_seconds}s"
            )
            return sched

        except ImportError:
            logger.warning(
                "apscheduler chua cai. Cai: pip install apscheduler==3.10.4\n"
                "Su dung start_blocking() thay the."
            )
            return None

    def change_ticker(self, new_ticker: str):
        """
        Thay doi ticker dang phan tich.
        Tao lai engine moi cho ticker moi.
        """
        old = self.ticker
        self.ticker       = new_ticker.upper()
        self._cycle_count = 0

        if self._engine:
            try:
                self._engine.shutdown()
            except Exception:
                pass
            self._engine = None

        logger.info(f"Thay doi ticker: {old} → {self.ticker}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="FinSent-Agent Realtime VMSI Scheduler"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default="SHB",
        help="Ma co phieu can phan tich (VD: SHB, VCB, VNINDEX)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1800,
        help="Chu ky phan tich (giay, mac dinh 1800 = 30 phut)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Chi chay 1 chu ky roi thoat (de test)",
    )
    args = parser.parse_args()

    scheduler = RealtimeScheduler(
        ticker=args.ticker,
        interval_seconds=args.interval,
    )

    if args.once:
        logger.info(f"Chay 1 chu ky don cho ticker={args.ticker}...")
        result = scheduler._run_single_cycle()
        import json
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        scheduler.start_blocking()


if __name__ == "__main__":
    main()
