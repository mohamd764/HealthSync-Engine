"""
Renpho Health → Google Sheets
تشغيل يومي لاستخراج بيانات الأصدقاء/العائلة وتصديرها إلى Google Sheet
"""
from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

from extractor import RenphoExtractor
from sheets_export import export_from_env

load_dotenv()


def main() -> int:
    email = os.getenv("RENPHO_EMAIL")
    password = os.getenv("RENPHO_PASSWORD")

    if not email or not password:
        print("خطأ: حدد RENPHO_EMAIL و RENPHO_PASSWORD في ملف .env")
        return 1

    print("جاري الاتصال بـ Renpho...")
    client = RenphoExtractor(email, password, debug=False)

    try:
        rows = client.get_all_client_data()
    except Exception as e:
        print(f"خطأ في استخراج البيانات: {e}")
        return 1

    if not rows:
        print("لم يتم العثور على بيانات.")
        return 0

    print(f"تم استخراج {len(rows)} سجل.")

    # تصدير إلى Google Sheet إذا كانت الإعدادات متوفرة
    if os.getenv("GOOGLE_SHEET_ID") and os.path.exists(
        os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")
    ):
        try:
            export_from_env(rows)
            print("تم التصدير إلى Google Sheet بنجاح.")
        except Exception as e:
            print(f"خطأ في التصدير إلى Google Sheet: {e}")
            return 1
    else:
        # طباعة النتائج فقط
        print("\n--- العينة الأولى ---")
        for r in rows[:10]:
            print(f"  {r['client_name']} | {r['weight_kg']} kg | {r['date_recorded']}")
        print("\nلتفعيل Google Sheet: أضف GOOGLE_SHEET_ID و credentials.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
