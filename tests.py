#!/usr/bin/env python3
"""
TCCHE Dashboard – Comprehensive Test Suite
===========================================
Validates imports, data loading, page layouts, callbacks, routing,
helper functions, and the full Dash app assembly.

Usage:
    py tests.py            # run all tests
    py tests.py -v         # verbose output
    py tests.py -k config  # run only tests matching "config"
"""

import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("MPLCONFIGDIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "matplotlib"))
os.environ.setdefault("MPLBACKEND", "Agg")

ROOT = Path(__file__).resolve().parent
_t0 = time.time()


# ────────────────────────────────────────────────────────────
# 1. MODULE IMPORTS
# ────────────────────────────────────────────────────────────

class TestImports(unittest.TestCase):
    """Every module must import without raising."""

    def test_import_config(self):
        import config  # noqa: F811
        self.assertTrue(hasattr(config, "COLORS"))
        self.assertTrue(hasattr(config, "FONT"))
        self.assertTrue(hasattr(config, "PLOT_LAYOUT"))

    def test_import_data_loader(self):
        import data_loader  # noqa: F811
        self.assertTrue(hasattr(data_loader, "hist_df"))
        self.assertTrue(hasattr(data_loader, "pred_df"))
        self.assertTrue(hasattr(data_loader, "metrics_df"))

    def test_import_agent(self):
        import agent  # noqa: F811
        self.assertTrue(hasattr(agent, "DISPLAY_CURRENCY"))

    def test_import_db(self):
        import db  # noqa: F811
        self.assertTrue(callable(getattr(db, "test_connection", None)))

    def test_import_auth(self):
        import auth  # noqa: F811
        self.assertTrue(callable(getattr(auth, "setup_auth", None)))

    def test_import_order_bumps(self):
        import order_bumps  # noqa: F811
        self.assertTrue(hasattr(order_bumps, "_WC_URL"))

    def test_import_hubspot_forms(self):
        import hubspot_forms  # noqa: F811
        self.assertTrue(hasattr(hubspot_forms, "FORM_DEFINITIONS"))

    def test_import_page_stock_manager(self):
        from pages import stock_manager
        self.assertTrue(callable(getattr(stock_manager, "layout", None)))

    def test_import_page_forms_manager(self):
        from pages import forms_manager
        self.assertTrue(callable(getattr(forms_manager, "layout", None)))

    def test_import_page_settings(self):
        from pages import settings
        self.assertTrue(callable(getattr(settings, "layout", None)))

    def test_import_page_cross_sell(self):
        from pages import cross_sell
        self.assertTrue(callable(getattr(cross_sell, "layout", None)))

    def test_import_page_reports(self):
        from pages import reports
        self.assertIsNotNone(reports)

    def test_import_page_main_dashboard(self):
        from pages import main_dashboard
        self.assertTrue(callable(getattr(main_dashboard, "layout", None)))

    def test_import_app(self):
        import app  # noqa: F811
        self.assertTrue(hasattr(app, "app"))
        self.assertTrue(hasattr(app, "server"))


# ────────────────────────────────────────────────────────────
# 2. CONFIG MODULE
# ────────────────────────────────────────────────────────────

class TestConfig(unittest.TestCase):
    """Validate config exports and helpers."""

    def setUp(self):
        import config
        self.cfg = config

    def test_colors_has_required_keys(self):
        required = {"bg", "card", "card_border", "text", "text_muted",
                     "accent", "accent2", "accent3", "accent4", "red", "grid"}
        self.assertTrue(required.issubset(set(self.cfg.COLORS.keys())))

    def test_plot_layout_is_dict(self):
        self.assertIsInstance(self.cfg.PLOT_LAYOUT, dict)
        self.assertIn("font", self.cfg.PLOT_LAYOUT)

    def test_category_colors_non_empty(self):
        self.assertGreater(len(self.cfg.CATEGORY_COLORS), 5)

    def test_parse_categories_basic(self):
        result = self.cfg.parse_categories("EVENTS|LIVESTREAM|My Event")
        self.assertEqual(result, ["EVENTS", "LIVESTREAM", "My Event"])

    def test_parse_categories_empty(self):
        result = self.cfg.parse_categories("")
        self.assertEqual(result, ["Uncategorized"])

    def test_parse_categories_nan(self):
        import math
        result = self.cfg.parse_categories(float("nan"))
        self.assertEqual(result, ["Uncategorized"])

    def test_card_style_returns_dict(self):
        style = self.cfg.card_style()
        self.assertIsInstance(style, dict)
        self.assertIn("backgroundColor", style)
        self.assertIn("borderRadius", style)

    def test_card_style_with_extra(self):
        style = self.cfg.card_style({"width": "100px"})
        self.assertEqual(style["width"], "100px")

    def test_section_label_returns_dash_component(self):
        from dash import html
        label = self.cfg.section_label("Test")
        self.assertIsInstance(label, html.P)

    def test_kpi_card_returns_dash_component(self):
        from dash import html
        card = self.cfg.kpi_card("Revenue", "$1,000")
        self.assertIsInstance(card, html.Div)

    def test_th_style_returns_dict(self):
        style = self.cfg._th_style()
        self.assertIn("padding", style)

    def test_td_style_returns_dict(self):
        style = self.cfg._td_style()
        self.assertIn("padding", style)

    def test_build_product_cat_map(self):
        import pandas as pd
        df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "category": ["EVENTS|Concert", "ONLINE COURSE", "EVENTS"],
        })
        mapping = self.cfg.build_product_cat_map(df)
        self.assertIn(1, mapping)
        self.assertIn("Concert", mapping[1])
        self.assertIn("EVENTS", mapping[1])

    def test_product_matches_cats(self):
        cat_map = {1: {"EVENTS", "Concert"}, 2: {"ONLINE COURSE"}}
        self.assertTrue(self.cfg.product_matches_cats(1, ["Concert"], cat_map))
        self.assertFalse(self.cfg.product_matches_cats(2, ["Concert"], cat_map))

    def test_filter_by_categories(self):
        import pandas as pd
        df = pd.DataFrame({"product_id": [1, 2, 3], "value": [10, 20, 30]})
        cat_map = {1: {"A"}, 2: {"B"}, 3: {"A", "B"}}
        filtered = self.cfg.filter_by_categories(df, ["A"], cat_map)
        self.assertEqual(set(filtered["product_id"]), {1, 3})

    def test_explode_categories(self):
        import pandas as pd
        df = pd.DataFrame({"product_id": [1], "category": ["A|B|C"]})
        exploded = self.cfg.explode_categories(df)
        self.assertEqual(len(exploded), 3)
        self.assertEqual(list(exploded["cat_single"]), ["A", "B", "C"])

    def test_generic_cats_is_frozenset(self):
        self.assertIsInstance(self.cfg.GENERIC_CATS, frozenset)
        self.assertIn("Uncategorized", self.cfg.GENERIC_CATS)


# ────────────────────────────────────────────────────────────
# 3. DATA LOADER
# ────────────────────────────────────────────────────────────

class TestDataLoader(unittest.TestCase):
    """Validate data loading results and derived globals."""

    def setUp(self):
        import data_loader
        self.dl = data_loader

    def test_hist_df_not_empty(self):
        self.assertGreater(len(self.dl.hist_df), 0)

    def test_hist_df_required_columns(self):
        required = {"order_date", "product_id", "product_name", "quantity_sold", "revenue", "category"}
        self.assertTrue(required.issubset(set(self.dl.hist_df.columns)),
                        f"Missing: {required - set(self.dl.hist_df.columns)}")

    def test_pred_df_not_empty(self):
        self.assertGreater(len(self.dl.pred_df), 0)

    def test_pred_df_has_predictions(self):
        self.assertIn("predicted_quantity", self.dl.pred_df.columns)

    def test_metrics_df_loaded(self):
        self.assertIsNotNone(self.dl.metrics_df)

    def test_exchange_rates_dict(self):
        self.assertIsInstance(self.dl.exchange_rates, dict)
        self.assertIn(self.dl.DISPLAY_CURRENCY, self.dl.exchange_rates)

    def test_event_status_map_not_empty(self):
        self.assertGreater(len(self.dl.event_status_map), 0)
        valid_statuses = {"active", "past", "course"}
        for status in self.dl.event_status_map.values():
            self.assertIn(status, valid_statuses)

    def test_all_categories_is_list(self):
        self.assertIsInstance(self.dl.all_categories, list)
        self.assertGreater(len(self.dl.all_categories), 0)

    def test_product_sales_dataframe(self):
        self.assertGreater(len(self.dl.product_sales), 0)
        self.assertIn("product_name", self.dl.product_sales.columns)

    def test_kpi_values_positive(self):
        self.assertGreater(self.dl.total_products, 0)
        self.assertGreater(self.dl.total_sales_qty, 0)
        self.assertGreater(self.dl.total_revenue, 0)

    def test_date_range_format(self):
        self.assertRegex(self.dl.date_min, r"\d{2}/\d{2}/\d{4}")
        self.assertRegex(self.dl.date_max, r"\d{2}/\d{2}/\d{4}")

    def test_filter_by_event_tab(self):
        active_df = self.dl.filter_by_event_tab(self.dl.hist_df, "active")
        self.assertLessEqual(len(active_df), len(self.dl.hist_df))

    def test_filter_by_currency(self):
        if "currency" in self.dl.hist_df.columns:
            first_cur = self.dl.hist_df["currency"].iloc[0]
            filtered = self.dl.filter_by_currency(self.dl.hist_df, [first_cur])
            self.assertTrue((filtered["currency"] == first_cur).all())

    def test_filter_by_currency_empty_list(self):
        result = self.dl.filter_by_currency(self.dl.hist_df, [])
        self.assertEqual(len(result), len(self.dl.hist_df))

    def test_lazy_loaders_callable(self):
        self.assertTrue(callable(self.dl.get_hourly_df))
        self.assertTrue(callable(self.dl.get_low_stock_df))
        self.assertTrue(callable(self.dl.get_source_df))
        self.assertTrue(callable(self.dl.get_cross_sell_df))
        self.assertTrue(callable(self.dl.get_geo_sales_df))

    def test_get_hourly_df_returns_dataframe(self):
        import pandas as pd
        df = self.dl.get_hourly_df()
        self.assertIsInstance(df, pd.DataFrame)

    def test_get_low_stock_df_returns_dataframe(self):
        import pandas as pd
        df = self.dl.get_low_stock_df()
        self.assertIsInstance(df, pd.DataFrame)

    def test_invalidate_lazy_cache(self):
        self.dl.get_hourly_df()
        self.assertIn("hourly_df", self.dl._lazy_cache)
        self.dl.invalidate_lazy_cache()
        self.assertEqual(len(self.dl._lazy_cache), 0)

    def test_all_orders_df(self):
        import pandas as pd
        self.assertIsInstance(self.dl.all_orders_df, pd.DataFrame)

    def test_product_cat_map(self):
        self.assertIsInstance(self.dl.product_cat_map, dict)

    def test_display_currency(self):
        self.assertIn(self.dl.DISPLAY_CURRENCY, ("USD", "CAD", "EUR", "GBP", "BRL"))

    def test_convert_revenue_no_crash(self):
        import pandas as pd
        df = pd.DataFrame({
            "revenue": [100.0, 200.0],
            "currency": ["USD", "CAD"],
        })
        result = self.dl.convert_revenue(df, self.dl.exchange_rates)
        self.assertIsInstance(result, pd.DataFrame)


# ────────────────────────────────────────────────────────────
# 4. PAGE LAYOUTS
# ────────────────────────────────────────────────────────────

class TestPageLayouts(unittest.TestCase):
    """Every page layout() must return a non-empty list of Dash components."""

    def _assert_layout(self, module_name):
        from pages import stock_manager, forms_manager, settings, cross_sell, main_dashboard
        modules = {
            "stock_manager": stock_manager,
            "forms_manager": forms_manager,
            "settings": settings,
            "cross_sell": cross_sell,
            "main_dashboard": main_dashboard,
        }
        mod = modules[module_name]
        children = mod.layout()
        self.assertIsInstance(children, list, f"{module_name}.layout() must return a list")
        self.assertGreater(len(children), 0, f"{module_name}.layout() returned empty list")

    def test_stock_manager_layout(self):
        self._assert_layout("stock_manager")

    def test_forms_manager_layout(self):
        self._assert_layout("forms_manager")

    def test_settings_layout(self):
        self._assert_layout("settings")

    def test_cross_sell_layout(self):
        self._assert_layout("cross_sell")

    def test_main_dashboard_layout(self):
        self._assert_layout("main_dashboard")


# ────────────────────────────────────────────────────────────
# 5. DASH APP ASSEMBLY
# ────────────────────────────────────────────────────────────

class TestAppAssembly(unittest.TestCase):
    """Validate the Dash app builds correctly."""

    def setUp(self):
        import app as _app
        self.app_mod = _app

    def test_app_instance_exists(self):
        from dash import Dash
        self.assertIsInstance(self.app_mod.app, Dash)

    def test_server_is_flask(self):
        from flask import Flask
        self.assertIsInstance(self.app_mod.server, Flask)

    def test_app_has_layout(self):
        self.assertIsNotNone(self.app_mod.app.layout)

    def test_app_title(self):
        self.assertIn("TCCHE", self.app_mod.app.title)

    def test_suppress_callback_exceptions(self):
        self.assertTrue(self.app_mod.app.config.suppress_callback_exceptions)


# ────────────────────────────────────────────────────────────
# 6. CALLBACKS REGISTRATION
# ────────────────────────────────────────────────────────────

class TestCallbacksExist(unittest.TestCase):
    """Validate that critical callback functions are defined in the right modules."""

    def test_route_page(self):
        import app as _app
        self.assertTrue(callable(getattr(_app, "route_page", None)))

    def test_main_dashboard_callbacks(self):
        from pages import main_dashboard as md
        for fn in ["update_filters", "update_kpis", "update_daily_report",
                    "start_sync", "poll_sync_progress", "reload_after_sync",
                    "handle_chat", "update_sales_map", "update_orders_table"]:
            self.assertTrue(callable(getattr(md, fn, None)), f"Missing: {fn}")

    def test_stock_manager_callbacks(self):
        from pages import stock_manager as sm
        for fn in ["load_stock_picker_options", "render_stock_manager_table",
                    "add_product_to_stock_manager", "run_auto_replenish"]:
            self.assertTrue(callable(getattr(sm, fn, None)), f"Missing: {fn}")

    def test_forms_manager_callbacks(self):
        from pages import forms_manager as fm
        for fn in ["render_forms_assignment_table", "handle_form_toggle",
                    "push_forms_to_hubspot", "sync_from_hubspot"]:
            self.assertTrue(callable(getattr(fm, fn, None)), f"Missing: {fn}")

    def test_settings_callbacks(self):
        from pages import settings as st
        for fn in ["load_user_permissions", "enforce_permissions",
                    "render_users_table", "render_roles_table", "change_password"]:
            self.assertTrue(callable(getattr(st, fn, None)), f"Missing: {fn}")

    def test_cross_sell_callbacks(self):
        from pages import cross_sell as cs
        for fn in ["render_crosssell_table", "render_crosssell_chart",
                    "render_order_bump_section", "render_ob_analytics",
                    "handle_autofill"]:
            self.assertTrue(callable(getattr(cs, fn, None)), f"Missing: {fn}")

    def test_reports_callbacks(self):
        from pages import reports as rp
        for fn in ["generate_report_content", "download_report_pdf"]:
            self.assertTrue(callable(getattr(rp, fn, None)), f"Missing: {fn}")


# ────────────────────────────────────────────────────────────
# 7. URL ROUTING
# ────────────────────────────────────────────────────────────

class TestRouting(unittest.TestCase):
    """Validate the URL routing callback logic."""

    def setUp(self):
        import app as _app
        self.route_page = _app.route_page

    def test_root_shows_dashboard(self):
        result = self.route_page("/")
        self.assertIn("block", str(result[0]))
        for page_style in result[1:]:
            self.assertIn("none", str(page_style))

    def test_stock_route(self):
        result = self.route_page("/stock")
        self.assertIn("none", str(result[0]))
        self.assertIn("block", str(result[1]))

    def test_forms_route(self):
        result = self.route_page("/forms")
        self.assertIn("none", str(result[0]))
        self.assertIn("block", str(result[2]))

    def test_crosssell_route(self):
        result = self.route_page("/cross-sell")
        self.assertIn("none", str(result[0]))
        self.assertIn("block", str(result[3]))

    def test_settings_route(self):
        result = self.route_page("/settings")
        self.assertIn("none", str(result[0]))
        self.assertIn("block", str(result[4]))

    def test_unknown_route_shows_dashboard(self):
        result = self.route_page("/unknown-page")
        self.assertIn("block", str(result[0]))


# ────────────────────────────────────────────────────────────
# 8. DATABASE & AUTH
# ────────────────────────────────────────────────────────────

class TestDatabaseConnection(unittest.TestCase):
    """Validate database connectivity."""

    def test_db_connection(self):
        import db
        self.assertTrue(db.test_connection(), "Database connection failed")

    def test_load_for_dashboard(self):
        import db
        hist, pred, metrics = db.load_for_dashboard()
        self.assertGreater(len(hist), 0)

    def test_load_all_orders(self):
        import db
        orders = db.load_all_orders()
        self.assertGreater(len(orders), 0)


class TestAuth(unittest.TestCase):
    """Validate auth module basics."""

    def test_jwt_secret_set(self):
        import auth
        self.assertTrue(len(auth.JWT_SECRET) > 10)

    def test_hash_password(self):
        import auth
        pw = "test_password_123"
        hashed = auth.hash_password(pw)
        self.assertIsInstance(hashed, str)
        self.assertNotEqual(hashed, pw)


# ────────────────────────────────────────────────────────────
# 9. EXTERNAL SERVICES (non-destructive checks)
# ────────────────────────────────────────────────────────────

class TestExternalServices(unittest.TestCase):
    """Validate external service configuration (no writes)."""

    def test_woocommerce_credentials_set(self):
        wc_url = os.getenv("WOOCOMMERCE_URL", "")
        wc_key = os.getenv("WOOCOMMERCE_KEY", "")
        self.assertTrue(len(wc_url) > 0, "WOOCOMMERCE_URL not set")
        self.assertTrue(len(wc_key) > 0, "WOOCOMMERCE_KEY not set")

    def test_openai_key_set(self):
        key = os.getenv("OPENAI_API_KEY") or os.getenv("OpenAI_API_KEY", "")
        self.assertTrue(len(key) > 0, "OpenAI API key not set")

    def test_hubspot_token_set(self):
        token = os.getenv("HUBSPOT_ACCESS_TOKEN", "")
        self.assertTrue(len(token) > 0, "HUBSPOT_ACCESS_TOKEN not set")

    def test_google_maps_key_set(self):
        key = os.getenv("GOOGLE_MAPS_API_KEY", "")
        self.assertTrue(len(key) > 0, "GOOGLE_MAPS_API_KEY not set")


# ────────────────────────────────────────────────────────────
# 10. SYNC INFRASTRUCTURE
# ────────────────────────────────────────────────────────────

class TestSyncInfrastructure(unittest.TestCase):
    """Validate sync thread setup (without actually running sync)."""

    def test_main_py_exists(self):
        self.assertTrue((ROOT / "main.py").exists(), "main.py not found")

    def test_sync_state_accessible(self):
        from pages import main_dashboard
        self.assertIsInstance(main_dashboard._sync_state, dict)
        self.assertIn("running", main_dashboard._sync_state)
        self.assertIn("exit_code", main_dashboard._sync_state)

    def test_sync_log_file_path(self):
        from pages import main_dashboard
        self.assertTrue(len(main_dashboard._SYNC_LOG_FILE) > 0)

    def test_data_dir_points_to_root(self):
        from pages import main_dashboard
        data_dir = main_dashboard.DATA_DIR
        self.assertTrue((data_dir / "main.py").exists(),
                        f"DATA_DIR ({data_dir}) does not contain main.py")

    def test_sys_executable_available(self):
        from pages import main_dashboard
        self.assertTrue(hasattr(sys, "executable"))


# ────────────────────────────────────────────────────────────
# 11. ORDER BUMPS MODULE
# ────────────────────────────────────────────────────────────

class TestOrderBumps(unittest.TestCase):
    """Validate order_bumps helper functions."""

    def test_wc_credentials(self):
        import order_bumps
        self.assertTrue(len(order_bumps._WC_URL) > 0)

    def test_generate_bump_copy_callable(self):
        import order_bumps
        self.assertTrue(callable(order_bumps.generate_bump_copy))

    def test_list_bumps_callable(self):
        import order_bumps
        self.assertTrue(callable(order_bumps.list_bumps))

    def test_is_configured_callable(self):
        import order_bumps
        self.assertTrue(callable(order_bumps.is_configured))


# ────────────────────────────────────────────────────────────
# 12. FILE STRUCTURE
# ────────────────────────────────────────────────────────────

class TestFileStructure(unittest.TestCase):
    """Validate the expected modular file structure exists."""

    EXPECTED_FILES = [
        "app.py",
        "config.py",
        "data_loader.py",
        "db.py",
        "auth.py",
        "agent.py",
        "order_bumps.py",
        "hubspot_forms.py",
        "main.py",
        "requirements.txt",
        "render.yaml",
        "pages/__init__.py",
        "pages/main_dashboard.py",
        "pages/stock_manager.py",
        "pages/forms_manager.py",
        "pages/settings.py",
        "pages/cross_sell.py",
        "pages/reports.py",
    ]

    def test_all_expected_files_exist(self):
        missing = [f for f in self.EXPECTED_FILES if not (ROOT / f).exists()]
        self.assertEqual(missing, [], f"Missing files: {missing}")

    def test_no_old_dashboard_py(self):
        self.assertFalse(
            (ROOT / "dashboard.py").exists(),
            "dashboard.py should not exist (renamed to app.py)"
        )

    def test_render_yaml_uses_app(self):
        content = (ROOT / "render.yaml").read_text()
        self.assertIn("app:server", content,
                      "render.yaml should reference app:server, not dashboard:server")
        self.assertNotIn("dashboard:server", content)

    def test_env_file_exists(self):
        self.assertTrue((ROOT / ".env").exists(), ".env file not found")

    def test_no_secrets_in_source(self):
        """Ensure no hardcoded API keys in committed Python files."""
        patterns = ["pat-eu" + "1-", "sk-pr" + "oj-", "ck_54" + "33", "cs_19" + "6a"]
        skip = {"tests.py", "_test_stock.py"}
        for py_file in ROOT.glob("*.py"):
            if py_file.name in skip:
                continue
            content = py_file.read_text(encoding="utf-8", errors="ignore")
            for pattern in patterns:
                self.assertNotIn(
                    pattern, content,
                    f"Possible hardcoded secret ({pattern}...) found in {py_file.name}"
                )
        for py_file in (ROOT / "pages").glob("*.py"):
            content = py_file.read_text(encoding="utf-8", errors="ignore")
            for pattern in patterns:
                self.assertNotIn(
                    pattern, content,
                    f"Possible hardcoded secret ({pattern}...) found in pages/{py_file.name}"
                )


# ────────────────────────────────────────────────────────────
# RUNNER
# ────────────────────────────────────────────────────────────

class _ColorResult(unittest.TextTestResult):
    """Custom result class with colored output."""

    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    def addSuccess(self, test):
        super().addSuccess(test)
        if self.showAll:
            self.stream.write(f"  {self.GREEN}PASS{self.RESET}\n")
        else:
            self.stream.write(f"{self.GREEN}.{self.RESET}")
            self.stream.flush()

    def addFailure(self, test, err):
        super().addFailure(test, err)
        if self.showAll:
            self.stream.write(f"  {self.RED}FAIL{self.RESET}\n")
        else:
            self.stream.write(f"{self.RED}F{self.RESET}")
            self.stream.flush()

    def addError(self, test, err):
        super().addError(test, err)
        if self.showAll:
            self.stream.write(f"  {self.RED}ERROR{self.RESET}\n")
        else:
            self.stream.write(f"{self.RED}E{self.RESET}")
            self.stream.flush()

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        if self.showAll:
            self.stream.write(f"  {self.YELLOW}SKIP{self.RESET} ({reason})\n")
        else:
            self.stream.write(f"{self.YELLOW}s{self.RESET}")
            self.stream.flush()


class _ColorRunner(unittest.TextTestRunner):
    resultclass = _ColorResult


def _print_banner():
    C = _ColorResult
    print(f"\n{C.CYAN}{C.BOLD}{'=' * 60}")
    print(f"  TCCHE Dashboard - Test Suite")
    print(f"{'=' * 60}{C.RESET}\n")


def _print_summary(result, elapsed):
    C = _ColorResult
    total = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped)
    passed = total - failures - errors - skipped

    print(f"\n{C.CYAN}{C.BOLD}{'-' * 60}")
    print(f"  RESULTS")
    print(f"{'-' * 60}{C.RESET}")
    print(f"  Total:    {total}")
    print(f"  {C.GREEN}Passed:   {passed}{C.RESET}")
    if failures:
        print(f"  {C.RED}Failed:   {failures}{C.RESET}")
    if errors:
        print(f"  {C.RED}Errors:   {errors}{C.RESET}")
    if skipped:
        print(f"  {C.YELLOW}Skipped:  {skipped}{C.RESET}")
    print(f"  Time:     {elapsed:.2f}s")

    if failures or errors:
        print(f"\n  {C.RED}{C.BOLD}SOME TESTS FAILED{C.RESET}")
    else:
        print(f"\n  {C.GREEN}{C.BOLD}ALL TESTS PASSED{C.RESET}")
    print()


if __name__ == "__main__":
    _print_banner()

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    if len(sys.argv) > 1 and sys.argv[1] == "-k":
        pattern = sys.argv[2] if len(sys.argv) > 2 else ""
        for test_class in [
            TestImports, TestConfig, TestDataLoader, TestPageLayouts,
            TestAppAssembly, TestCallbacksExist, TestRouting,
            TestDatabaseConnection, TestAuth, TestExternalServices,
            TestSyncInfrastructure, TestOrderBumps, TestFileStructure,
        ]:
            for test in loader.loadTestsFromTestCase(test_class):
                if pattern.lower() in str(test).lower():
                    suite.addTest(test)
    else:
        for test_class in [
            TestImports, TestConfig, TestDataLoader, TestPageLayouts,
            TestAppAssembly, TestCallbacksExist, TestRouting,
            TestDatabaseConnection, TestAuth, TestExternalServices,
            TestSyncInfrastructure, TestOrderBumps, TestFileStructure,
        ]:
            suite.addTests(loader.loadTestsFromTestCase(test_class))

    verbosity = 2 if "-v" in sys.argv else 1
    runner = _ColorRunner(verbosity=verbosity)
    result = runner.run(suite)

    elapsed = time.time() - _t0
    _print_summary(result, elapsed)

    sys.exit(0 if result.wasSuccessful() else 1)
