#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metr_specs_scraper.py

Масовий парсинг структурованої таблиці "Характеристики" з сайту постачальника
metr-plus.com.ua і запис результату у колонку products.metr_specs (jsonb) в Supabase.

Логіка:
1. Тягнемо з Supabase всі товари категорій 77 / 494 / 347037, у яких metr_specs IS NULL.
2. Для кожної потрібної категорії обходимо лістинг товарів постачальника
   (https://metr-plus.com.ua/index.php?section=<cat>&lang=uk&page=N), збираючи
   мапу SKU -> product_id_на_сайті (парсинг не потребує окремого пошуку/логіну —
   артикул видно прямо в назві товару в лістингу, сайт публічний).
3. Для кожного нашого товару шукаємо збіг по SKU (точний, після trim) і, якщо
   знайдено, завантажуємо сторінку товару, розбираємо таблицю "Характеристики"
   у структурований dict {секція: {підпараметр: значення}} (значення може бути
   списком рядків, якщо в комірці кілька рядків через <br>, напр. Мотор).
4. Записуємо результат у products.metr_specs через Supabase REST (PATCH).
   Тригер trg_products_auto_parse в базі сам підхопить оновлення і викличе
   parse_metr_specs() (основний парсер) + parse_single_product() (підстраховка
   з опису, заповнює лише прогалини) — нічого додатково запускати не треба.

Обережність:
- Сайт може банити IP за надто часті запити ("Ваша IP адреса заблокована"),
  тому витримуємо паузу між запитами (~4-9с базово, зрідка 25-70с "людська"
  перерва, зрідка коротша, плюс гарантована довша пауза кожні ~40 запитів) і
  робимо backoff/retry при 403/429/5xx.
- Кодування сторінок — windows-1251, не utf-8.
- Прогрес логуємо, щоб у разі падіння (rate limit, збій мережі) можна було
  перезапустити скрипт — товари з уже заповненим metr_specs просто не
  потраплять в вибірку на наступному запуску.

Потрібні змінні середовища (ті самі секрети, що вже налаштовані в GitHub Actions
для основного happyland-скрипта):
  SUPABASE_URL               -- https://<ref>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY  -- service_role ключ (потрібен, бо RLS може блокувати anon-запис)

Залежності: requests, beautifulsoup4
  pip install requests beautifulsoup4
"""

import os
import re
import sys
import time
import json
import logging
import random
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString

# ---------------------------------------------------------------------------
# Конфігурація
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: потрібні змінні середовища SUPABASE_URL і SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
    sys.exit(1)

SITE_BASE = "https://metr-plus.com.ua"
CATEGORIES = [77, 494, 347037]  # ті самі category_id, що й section= на сайті постачальника

MAX_RETRIES = 4
BACKOFF_BASE = 8                      # секунд, зростає з кожною спробою

# --- Пауза між запитами до сайту постачальника -----------------------------
# Робимо її помітно більшою і НЕ рівномірною: більшість пауз — у "базовому"
# діапазоні, але з певним шансом трапляється довша "людська" перерва (ніби
# людина відволіклась), а зрідка — зовсім коротка. Це набагато важче відрізнити
# від живого трафіку, ніж стабільний random.uniform(a, b) на кожен запит.
BASE_DELAY_RANGE = (4.0, 9.0)         # звичайна пауза, сек
LONG_PAUSE_CHANCE = 0.08              # ймовірність "довгої" паузи
LONG_PAUSE_RANGE = (25.0, 70.0)
SHORT_PAUSE_CHANCE = 0.05             # ймовірність короткої паузи (рідко)
SHORT_PAUSE_RANGE = (1.5, 3.0)

# Кожні N запитів — додаткова гарантована "перекур"-пауза, незалежно від решти
EXTRA_PAUSE_EVERY = 40
EXTRA_PAUSE_RANGE = (40.0, 90.0)
_request_counter = 0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("metr_specs_scraper")


# ---------------------------------------------------------------------------
# HTTP helper з повторними спробами та бекофом
# ---------------------------------------------------------------------------

def fetch(session: requests.Session, url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
        except requests.RequestException as e:
            log.warning("Request error on %s (attempt %d): %s", url, attempt, e)
            time.sleep(BACKOFF_BASE * attempt)
            continue

        if resp.status_code == 200:
            resp.encoding = "windows-1251"
            text = resp.text
            if "заблокован" in text.lower() or "заблокована" in text.lower():
                log.error("IP заблоковано постачальником. Зупиняю скрипт достроково.")
                return None
            return text

        if resp.status_code in (403, 429, 500, 502, 503):
            wait = BACKOFF_BASE * attempt
            log.warning("HTTP %s on %s, retry in %ds (attempt %d/%d)",
                        resp.status_code, url, wait, attempt, MAX_RETRIES)
            time.sleep(wait)
            continue

        log.warning("Unexpected HTTP %s on %s", resp.status_code, url)
        return None

    log.error("Giving up on %s after %d attempts", url, MAX_RETRIES)
    return None


def polite_sleep():
    """Нерівномірна пауза між запитами — базова більшість часу, зрідка довша
    ("людина відволіклась") або коротша, плюс періодична гарантована перерва."""
    global _request_counter
    _request_counter += 1

    if _request_counter % EXTRA_PAUSE_EVERY == 0:
        delay = random.uniform(*EXTRA_PAUSE_RANGE)
        log.info("Планова довша перерва (%d запитів поспіль): %.1fs", EXTRA_PAUSE_EVERY, delay)
        time.sleep(delay)
        return

    r = random.random()
    if r < LONG_PAUSE_CHANCE:
        delay = random.uniform(*LONG_PAUSE_RANGE)
    elif r < LONG_PAUSE_CHANCE + SHORT_PAUSE_CHANCE:
        delay = random.uniform(*SHORT_PAUSE_RANGE)
    else:
        delay = random.uniform(*BASE_DELAY_RANGE)

    time.sleep(delay)


# ---------------------------------------------------------------------------
# Крок 1: тягнемо з Supabase товари, яким потрібен metr_specs
# ---------------------------------------------------------------------------

def supabase_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def fetch_products_needing_specs():
    products = []
    offset = 0
    limit = 1000
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/products"
            f"?select=id,sku,category_id"
            f"&category_id=in.({','.join(str(c) for c in CATEGORIES)})"
            f"&metr_specs=is.null"
            f"&offset={offset}&limit={limit}"
        )
        resp = requests.get(url, headers=supabase_headers(), timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        products.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return products


def update_metr_specs(product_id: str, specs: dict) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/products?id=eq.{product_id}"
    resp = requests.patch(
        url,
        headers=supabase_headers(),
        data=json.dumps({"metr_specs": specs}),
        timeout=30,
    )
    if resp.status_code not in (200, 204):
        log.error("Supabase update failed for %s: %s %s", product_id, resp.status_code, resp.text[:300])
        return False
    return True


# ---------------------------------------------------------------------------
# Крок 2: обхід лістингу категорії постачальника -> мапа SKU -> product URL
# ---------------------------------------------------------------------------

SKU_LINK_RE = re.compile(r"index\.php\?section=(\d+)&id=(\d+)")


def build_sku_map_for_category(session: requests.Session, category_id: int) -> dict:
    """Повертає {sku: product_id_на_сайті} для всієї категорії, обходячи пагінацію."""
    sku_map = {}
    page = 1
    seen_first_urls = set()

    while True:
        page_url = f"{SITE_BASE}/index.php?section={category_id}&lang=uk"
        if page > 1:
            page_url += f"&page={page}"

        html = fetch(session, page_url)
        polite_sleep()

        if html is None:
            log.warning("Не вдалось завантажити сторінку %d категорії %d — зупиняю обхід цієї категорії",
                        page, category_id)
            break

        soup = BeautifulSoup(html, "html.parser")

        first_product = None
        for a in soup.find_all("a", href=True):
            if SKU_LINK_RE.search(a["href"]):
                first_product = a
                break
        if first_product:
            first_href = first_product["href"]
            if first_href in seen_first_urls:
                log.info("Категорія %d: виявлено повтор останньої сторінки, завершую обхід.", category_id)
                break
            seen_first_urls.add(first_href)

        found_on_page = 0
        for a in soup.find_all("a", href=True):
            m = SKU_LINK_RE.search(a["href"])
            if not m:
                continue
            site_section, site_id = m.group(1), m.group(2)
            if int(site_section) != category_id:
                continue
            # Текст посилання зазвичай містить артикул (напр. "Джип JJ2022EBLR-1(24V)")
            link_text = a.get_text(" ", strip=True)
            sku_match = re.search(r"[A-Za-zА-Яа-яЇїІіЄєҐґ]*\s?\d[\w().\-]*", link_text)
            # Артикули на цьому сайті — переважно останній "токен" у тексті посилання,
            # що містить цифри; надійніше просто спробувати всі "слова" і звірити з нашою
            # базою пізніше по точному SKU з нашої сторони (див. match_products).
            if link_text:
                # Зберігаємо ВЕСЬ текст посилання як кандидата — звірка по точному SKU
                # відбудеться на етапі match_products() через прямий пошук підрядка.
                # ВАЖЛИВО: a['href'] на цьому сайті вже буває АБСОЛЮТНИМ URL
                # (https://metr-plus.com.ua/...), тому НЕ можна просто приклеювати
                # SITE_BASE спереду (вийде здвоєний домен) — використовуємо urljoin,
                # який коректно обробляє і відносні, і вже повні посилання.
                sku_map[link_text] = {"site_id": site_id, "url": urljoin(SITE_BASE + "/", a["href"])}
                found_on_page += 1

        log.info("Категорія %d, сторінка %d: знайдено %d посилань на товари", category_id, page, found_on_page)

        # Обмежник на випадок нескінченної пагінації через несподівану розмітку
        if page > 60:
            log.warning("Досягнуто ліміту 60 сторінок для категорії %d, зупиняюсь", category_id)
            break

        page += 1

    return sku_map


def find_product_url_by_sku(sku: str, sku_map: dict) -> Optional[str]:
    """Точний пошук SKU серед текстів посилань лістингу (без урахування регістру)."""
    sku_norm = sku.strip().lower()
    for link_text, info in sku_map.items():
        if sku_norm in link_text.lower():
            return info["url"]
    return None


# ---------------------------------------------------------------------------
# Крок 3: парсинг таблиці "Характеристики" на сторінці товару
# ---------------------------------------------------------------------------

def parse_specs_table(html: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "html.parser")

    # Таблиця характеристик — звичайна <table>, рядки якої мають (зазвичай) 3 <td>:
    # [Секція | Підпараметр | Значення], де Секція і/або Підпараметр можуть бути
    # порожні / "&nbsp;" у рядках-продовженнях тієй самої секції.
    candidate_tables = soup.find_all("table")
    if not candidate_tables:
        return None

    best_specs = None
    best_rows_count = 0

    for table in candidate_tables:
        specs: dict = {}
        current_section = None
        rows_parsed = 0

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            def cell_text(td):
                # Мотор та подібні поля можуть мати кілька рядків через <br>
                parts = []
                buf = []
                for node in td.children:
                    if getattr(node, "name", None) == "br":
                        parts.append("".join(buf).strip())
                        buf = []
                    elif isinstance(node, NavigableString):
                        buf.append(str(node))
                    else:
                        buf.append(node.get_text())
                parts.append("".join(buf).strip())
                parts = [p for p in parts if p]
                if not parts:
                    return ""
                return parts[0] if len(parts) == 1 else parts

            if len(cells) >= 3:
                section_raw = cells[0].get_text(strip=True)
                subparam_raw = cells[1].get_text(strip=True)
                value = cell_text(cells[2])
            else:
                section_raw = ""
                subparam_raw = cells[0].get_text(strip=True)
                value = cell_text(cells[1])

            section_raw = section_raw.replace("\xa0", "").strip()
            subparam_raw = subparam_raw.replace("\xa0", "").strip()

            if section_raw:
                current_section = section_raw

            if not current_section:
                continue
            if value == "" or value is None:
                continue

            key = subparam_raw if subparam_raw else "value"
            specs.setdefault(current_section, {})[key] = value
            rows_parsed += 1

        if rows_parsed > best_rows_count:
            best_specs = specs
            best_rows_count = rows_parsed

    # Дуже маленькі "таблиці" (меню, футер тощо) відсіюємо порогом
    if best_rows_count < 3:
        return None

    return best_specs


def verify_sku_on_page(html: str, expected_sku: str) -> bool:
    return expected_sku.strip().lower() in html.lower()


# ---------------------------------------------------------------------------
# Основний пайплайн
# ---------------------------------------------------------------------------

def main():
    log.info("Старт скрипта парсингу metr_specs")

    products = fetch_products_needing_specs()
    log.info("Товарів без metr_specs у категоріях %s: %d", CATEGORIES, len(products))
    if not products:
        log.info("Нема чого обробляти — завершую.")
        return

    by_category = {}
    for p in products:
        by_category.setdefault(p["category_id"], []).append(p)

    session = requests.Session()

    total_ok = 0
    total_notfound = 0
    total_failed = 0

    for category_id, cat_products in by_category.items():
        log.info("=== Категорія %s: %d товарів потребують metr_specs ===", category_id, len(cat_products))

        sku_map = build_sku_map_for_category(session, category_id)
        log.info("Категорія %s: у лістингу постачальника знайдено %d записів", category_id, len(sku_map))

        for p in cat_products:
            product_id = p["id"]
            sku = p["sku"]

            product_url = find_product_url_by_sku(sku, sku_map)
            if not product_url:
                log.info("SKU %s: не знайдено на сайті постачальника (категорія %s) — пропускаю", sku, category_id)
                total_notfound += 1
                continue

            html = fetch(session, product_url)
            polite_sleep()

            if html is None:
                log.warning("SKU %s: не вдалось завантажити сторінку товару", sku)
                total_failed += 1
                continue

            if not verify_sku_on_page(html, sku):
                log.warning("SKU %s: артикул не підтвердився на сторінці %s — пропускаю, щоб не записати чужі дані",
                            sku, product_url)
                total_notfound += 1
                continue

            specs = parse_specs_table(html)
            if not specs:
                log.warning("SKU %s: таблицю характеристик не знайдено/порожня на %s", sku, product_url)
                total_failed += 1
                continue

            if update_metr_specs(product_id, specs):
                log.info("SKU %s: metr_specs записано (%d секцій)", sku, len(specs))
                total_ok += 1
            else:
                total_failed += 1

    log.info("=== Готово. Успішно: %d, не знайдено на сайті: %d, помилки: %d ===",
              total_ok, total_notfound, total_failed)


if __name__ == "__main__":
    main()
