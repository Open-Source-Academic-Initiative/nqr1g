import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, patch

import httpx

from opensai_app.config import SOURCES, settings
from opensai_app.presentation import build_page_dataframe, build_results_view
from opensai_app.search_service import SearchService, build_where_clause
from opensai_app.socrata_client import SocrataClient


class SourcePartitionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = SocrataClient()
        self.calls = []
        self.integrated = [self.row("I-1", "SECOPI"), self.row("II-copia", "SECOPII")]
        self.direct = [self.row("II-1", "SECOPII")]
        self.failed_rows = None

    async def asyncTearDown(self):
        await self.client.close()

    @staticmethod
    def row(identifier, origin):
        return {"id_contrato": identifier, "row_id": identifier,
                "fecha": "2025-01-02T00:00:00", "origen_publicado": origin}

    async def reply(self, url, **kwargs):
        params = kwargs["params"]
        source = "SECOP_I" if SOURCES["SECOP_I"].dataset_id in url else "SECOP_II"
        self.calls.append((source, params))
        rows = self.integrated if source == "SECOP_I" else self.direct
        # Simula el filtro en el servidor; la aplicación recibe solo su proyección.
        if "origen = 'SECOPI'" in params["$where"]:
            rows = [r for r in rows if r["origen_publicado"] == "SECOPI"]
        is_count = params["$select"] == "count(*) as total"
        if not is_count and source == self.failed_rows:
            return httpx.Response(400, request=httpx.Request("GET", url), json=[])
        payload = ([{"total": str(len(rows))}] if is_count else
                   [{k: v for k, v in r.items() if k != "origen_publicado"}
                    for r in rows[:params["$limit"]]])
        return httpx.Response(200, request=httpx.Request("GET", url), json=payload)

    async def search(self, page=1):
        with patch.object(self.client._client, "get", AsyncMock(side_effect=self.reply)):
            return await SearchService(self.client).execute_search("Entidad de prueba", 2025, page, 2026)

    async def test_predicado_en_conteo_y_filas_solo_secop_i(self):
        result = await self.search()
        self.assertIsNone(result.error)
        self.assertEqual(len(self.calls), 4)
        for source, params in self.calls:
            before = build_where_clause(SOURCES[source].cols, "Entidad de prueba", 2025)
            if source == "SECOP_I":
                self.assertEqual(params["$where"], f"({before}) AND origen = 'SECOPI'")
            else:
                self.assertEqual(params["$where"], before)
                self.assertNotIn("origen", params["$where"])
            self.assertIn("2025-01-01T00:00:00", params["$where"])
            self.assertIn("2025-12-31T23:59:59", params["$where"])
            self.assertIn("ENTIDAD DE PRUEBA", params["$where"])
        for source in SOURCES:
            operations = [p["$select"] == "count(*) as total" for s, p in self.calls if s == source]
            self.assertEqual(operations, [True, False])

    async def test_rama_i_recibe_solo_origen_i_desde_el_servidor(self):
        result = await self.search()
        branch = result.final_df[result.final_df["Origen"] == "SECOP I"]
        self.assertEqual(branch["id_contrato"].tolist(), ["I-1"])
        self.assertEqual(set(result.final_df["id_contrato"]), {"I-1", "II-1"})
        self.assertEqual(result.count, 2)

    async def test_paginas_y_limites_derivan_del_conteo_particionado(self):
        self.integrated += [self.row("I-2", "SECOPI"), self.row("I-3", "SECOPI")]
        self.direct += [self.row("II-2", "SECOPII")]
        small = replace(settings, search=replace(settings.search, per_page=2))
        with patch("opensai_app.search_service.settings", small), patch("opensai_app.presentation.settings", small):
            for requested, actual, visible in ((1, 1, 2), (2, 2, 2), (3, 3, 1), (4, 3, 1)):
                with self.subTest(pagina=requested):
                    self.calls.clear()
                    result = await self.search(requested)
                    self.assertIsNone(result.error)
                    self.assertEqual((result.count, result.pages, result.current_page), (5, 3, actual))
                    page = build_page_dataframe(build_results_view(result.final_df), result.current_page)
                    self.assertEqual(len(page), visible)
                    limits = {s: p["$limit"] for s, p in self.calls if p["$select"] != "count(*) as total"}
                    self.assertEqual(limits, {"SECOP_I": min(3, actual * 2), "SECOP_II": 2})
                    self.assertEqual(len(self.calls), 4)

    async def test_conteo_i_cero_omite_filas_y_conserva_pagina_ii(self):
        self.integrated = [self.row("II-copia", "SECOPII")]
        result = await self.search()
        self.assertEqual((result.count, result.pages), (1, 1))
        self.assertEqual(result.final_df["Origen"].tolist(), ["SECOP II"])
        self.assertEqual(len(self.calls), 3)
        self.assertFalse(any(s == "SECOP_I" and p["$select"] != "count(*) as total" for s, p in self.calls))

    async def test_fallo_de_filas_de_cada_fuente_conserva_la_otra(self):
        for source in SOURCES:
            with self.subTest(fuente_fallida=source):
                self.failed_rows = source
                result = await self.search()
                self.assertIsNone(result.error)
                self.assertEqual((result.count, result.pages), (1, 1))
                self.assertNotIn(source.replace("_", " "), result.final_df["Origen"].tolist())
                self.assertTrue(any(source in warning for warning in result.warnings))
