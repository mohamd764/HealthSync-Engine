"""
Renpho Health Data Extractor
استخراج بيانات الاسم، الوزن (kg)، تاريخ التسجيل
يدعم: البيانات الشخصية + أعضاء العائلة/المقياس
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from renpho import RenphoClient


class RenphoExtractor(RenphoClient):
    """Extended client to fetch family/members and all measurements."""

    def get_all_client_data(self) -> list[dict[str, Any]]:
        """
        تجميع كل البيانات: الاسم، الوزن (kg)، تاريخ التسجيل.
        يرجع قائمة من: {"client_name": str, "weight_kg": float, "date_recorded": str}
        """
        rows: list[dict[str, Any]] = []
        name_map: dict[str, str] = {}

        if not self.token:
            self.login()

        # اسم الحساب الرئيسي من بيانات تسجيل الدخول
        name_map[str(self.user_id)] = (
            (self.user_info or {})
            .get("nickname")
            or (self.user_info or {}).get("accountName")
            or (self.user_info or {}).get("name")
            or str(self.user_id)
        )

        # 2. جلب device info والقياسات
        device_info = self.get_device_info()
        scales = device_info.get("scale", [])

        for scale in scales:
            table_name = scale.get("tableName")
            count = scale.get("count", 0)
            user_ids = scale.get("userIds", []) or [self.user_id]

            if not table_name or count == 0:
                continue

            for uid in user_ids:
                uid_str = str(uid)
                name = name_map.get(uid_str, f"Client {uid}")
                measurements = self.get_measurements(table_name, uid, count)

                for m in measurements:
                    weight = m.get("weight")
                    if weight is None:
                        continue
                    ts = m.get("timeStamp") or m.get("timestamp") or m.get("created_at")
                    if isinstance(ts, (int, float)):
                        dt = datetime.fromtimestamp(ts / 1000 if ts > 1e10 else ts)
                        date_str = dt.strftime("%Y-%m-%d")
                    else:
                        date_str = str(ts) if ts else ""

                    rows.append(
                        {
                            "client_name": name,
                            "weight_kg": round(float(weight), 2),
                            "date_recorded": date_str,
                        }
                    )

        # ترتيب حسب التاريخ (الأحدث أولاً)
        rows.sort(key=lambda r: (r["date_recorded"], r["client_name"]), reverse=True)

        # إزالة التكرارات (نفس الاسم + الوزن + التاريخ)
        seen: set[tuple[str, float, str]] = set()
        unique_rows: list[dict[str, Any]] = []
        for r in rows:
            key = (r["client_name"], r["weight_kg"], r["date_recorded"])
            if key not in seen:
                seen.add(key)
                unique_rows.append(r)

        return unique_rows
