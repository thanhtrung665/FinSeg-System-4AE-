#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_dependencies.py
Kiểm tra tất cả dependencies cần thiết cho realtime pipeline.
"""

import sys

print("=" * 60)
print("  Checking Dependencies")
print("=" * 60)

missing = []
installed = []

# Core packages
packages = [
    ("feedparser", "News RSS parsing"),
    ("beautifulsoup4", "HTML parsing"),
    ("lxml", "XML parsing"),
    ("requests", "HTTP requests"),
    ("kafka", "Kafka client"),
    ("chromadb", "Vector database"),
    ("pandas", "Data manipulation"),
    ("numpy", "Numerical computing"),
    ("sentence_transformers", "Embeddings"),
    ("vnstock", "Vietnam stock data"),
    ("streamlit", "Web dashboard"),
    ("plotly", "Charts"),
    ("playwright", "Browser automation"),
    ("apscheduler", "Scheduling"),
]

for pkg_name, description in packages:
    try:
        if pkg_name == "beautifulsoup4":
            import bs4
            installed.append((pkg_name, description, bs4.__version__))
        elif pkg_name == "kafka":
            import kafka
            installed.append((pkg_name, description, kafka.__version__))
        else:
            pkg = __import__(pkg_name.replace("-", "_"))
            version = getattr(pkg, "__version__", "unknown")
            installed.append((pkg_name, description, version))
        print(f"✅ {pkg_name:25s} {version:15s} - {description}")
    except ImportError:
        missing.append((pkg_name, description))
        print(f"❌ {pkg_name:25s} {'MISSING':15s} - {description}")

print("\n" + "=" * 60)

if missing:
    print(f"⚠️  Found {len(missing)} missing packages:")
    print("\nInstall command:")
    print("pip install " + " ".join(pkg for pkg, _ in missing))
    print("\nOr install from requirements:")
    print("pip install -r requirements.txt")
    print("pip install -r realtime_pipeline/requirements_realtime.txt")
    sys.exit(1)
else:
    print(f"✅ All {len(installed)} packages installed!")
    print("\nYou can now run:")
    print("  streamlit run dashboard_realtime.py --server.port 8502")
    print("  python realtime_pipeline/scheduler.py --ticker SHB --once")

print("=" * 60)
