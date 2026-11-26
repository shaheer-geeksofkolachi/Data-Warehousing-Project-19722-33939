"""Screenshots of the 4 Superset BI dashboards (headless Chrome via Playwright).
    python scripts/screenshot_bi.py <out_dir> [--chrome /usr/bin/google-chrome]
Logs in, opens every dashboard in standalone mode, waits until all charts finished loading, saves full-page PNGs and
reports any chart that shows an error."""
import os
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = os.getenv("SDI_SUPERSET_URL", "http://localhost:8088")
USER, PWD = os.getenv("SDI_SUPERSET_USER", "admin"), os.getenv("SDI_SUPERSET_PASSWORD", "admin")
SLUGS = ["sdi-bi-1-hospital", "sdi-bi-2-channel", "sdi-bi-3-velocity", "sdi-bi-4-unified"]


def main(out_dir, chrome=None):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    problems = {}
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=chrome, args=["--no-sandbox"]) if chrome else p.chromium.launch()
        page = b.new_page(viewport={"width": 1680, "height": 1050}, device_scale_factor=1)
        page.goto(f"{URL}/login/")
        page.fill("input#username", USER)
        page.fill("input#password", PWD)
        page.click("input[type=submit], button[type=submit]")
        page.wait_for_load_state("networkidle")
        for slug in SLUGS:
            page.goto(f"{URL}/superset/dashboard/{slug}/?standalone=1")
            page.wait_for_selector(".dashboard-component-chart-holder", timeout=60000)
            deadline = time.time() + 90
            while time.time() < deadline:      # wait until no chart is still loading
                if page.locator(".loading, .ant-skeleton-active").count() == 0:
                    break
                time.sleep(1)
            time.sleep(4)                       # echarts animations
            errs = page.locator(".dashboard-component-chart-holder .alert-danger, .dashboard-component-chart-holder "
                                "[data-test='chart-error'], .dashboard-component-chart-holder .error-alert")
            texts = [errs.nth(i).inner_text()[:200] for i in range(errs.count())]
            if texts:
                problems[slug] = texts
            page.screenshot(path=str(out / f"{slug}.png"), full_page=True)
            print(slug, "charts:", page.locator(".dashboard-component-chart-holder").count(), "errors:", len(texts))
        b.close()
    for k, v in problems.items():
        print("PROBLEMS", k, v)
    return problems


if __name__ == "__main__":
    chrome = sys.argv[sys.argv.index("--chrome") + 1] if "--chrome" in sys.argv else None
    sys.exit(1 if main(sys.argv[1] if len(sys.argv) > 1 else "docs/bi_screenshots", chrome) else 0)
