import time
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import AsyncMock, patch

import httpx

from opensai_app.config import SOURCES, settings
from opensai_app.search_service import SearchService, build_where_clause
from opensai_app.socrata_client import RequestBudgetExceeded, SocrataClient


class SocrataClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = SocrataClient()
        self.request = httpx.Request('GET', 'https://www.datos.gov.co/resource/rpmr-utcd.json')

    async def asyncTearDown(self):
        await self.client.close()

    def response(self, status, payload=None, headers=None):
        return httpx.Response(status, request=self.request, json=payload if payload is not None else [], headers=headers)

    async def test_proyeccion_por_fuente_conserva_datos_y_orden(self):
        for name, config in SOURCES.items():
            with self.subTest(fuente=name):
                row = {'row_id':'fila-1', 'url':'https://example.org/contrato', 'id_contrato':'1'}
                with patch.object(self.client._client, 'get', AsyncMock(return_value=self.response(200, [row]))) as get:
                    where = build_where_clause(config.cols, 'Entidad de prueba', 2025)
                    frame = await self.client.query_source_rows(name, config, where, 50)
                self.assertEqual(get.await_count, 1)
                params = get.call_args.kwargs['params']
                expected = 'url_contrato as url' if name == 'SECOP_I' else 'urlproceso.url as url'
                self.assertIn(expected, params['$select'])
                self.assertNotIn('url_contrato.url', params['$select'])
                self.assertEqual(params['$where'], where)
                self.assertEqual(params['$order'], f"{config.cols['fecha']} DESC, :id DESC")
                self.assertEqual(frame.iloc[0]['url'], row['url'])
                self.assertEqual(frame.iloc[0]['Origen'], name.replace('_', ' '))

    async def test_errores_permanentes_no_repiten_consulta(self):
        for status in (400, 401, 403, 404):
            with self.subTest(http=status), patch.object(self.client._client, 'get', AsyncMock(return_value=self.response(status))) as get:
                with self.assertRaises(httpx.HTTPStatusError):
                    await self.client.query_source_rows('SECOP_I', SOURCES['SECOP_I'], 'true', 1)
                self.assertEqual(get.await_count, 1)

    async def test_transitorios_recuperan_con_espera(self):
        for status in (202, 429, 500, 502, 503, 504):
            with self.subTest(http=status), patch.object(self.client._client, 'get', AsyncMock(side_effect=[self.response(status), self.response(200)])) as get, patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock) as sleep:
                result = await self.client.soda_get(str(self.request.url), {}, f'fuente-{status}')
                self.assertEqual(result.status_code, 200)
                self.assertEqual(get.await_count, 2)
                self.assertEqual(sleep.await_count, 1)
                self.assertGreater(sleep.call_args.args[0], 0)

    async def test_202_agotado_es_fallo(self):
        with patch.object(self.client._client, 'get', AsyncMock(return_value=self.response(202))) as get, patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock):
            with self.assertRaises(httpx.HTTPStatusError):
                await self.client.soda_get(str(self.request.url), {}, 'SECOP_I')
        self.assertEqual(get.await_count, settings.socrata.max_retries + 1)

    async def test_retry_after_largo_no_se_recorta_ni_reintenta(self):
        response = self.response(429, headers={'Retry-After':'120'})
        self.assertEqual(self.client.compute_retry_delay(response, 0), 120)
        with patch.object(self.client._client, 'get', AsyncMock(return_value=response)) as get, patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock) as sleep:
            with self.assertRaises(RequestBudgetExceeded):
                await self.client.soda_get(str(self.request.url), {}, 'SECOP_I')
        self.assertEqual(get.await_count, 1)
        sleep.assert_not_awaited()

    async def test_retry_after_excede_plazo_restante(self):
        response = self.response(429, headers={'Retry-After':'10'})
        with patch.object(self.client._client, 'get', AsyncMock(return_value=response)) as get, patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock) as sleep:
            with self.assertRaises(RequestBudgetExceeded):
                await self.client.soda_get(str(self.request.url), {}, 'SECOP_I', time.monotonic()+2)
        self.assertEqual(get.await_count, 1)
        sleep.assert_not_awaited()

    async def test_retry_after_valido_se_respeta(self):
        for delay in (0, 1):
            with self.subTest(espera=delay), patch.object(self.client._client, 'get', AsyncMock(side_effect=[self.response(429, headers={'Retry-After':str(delay)}), self.response(200)])), patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock) as sleep:
                await self.client.soda_get(str(self.request.url), {}, 'SECOP_I')
                sleep.assert_awaited_once_with(float(delay))

    async def test_retry_after_fecha_http_y_valor_invalido(self):
        future = format_datetime(datetime.now(timezone.utc)+timedelta(seconds=120), usegmt=True)
        delay = self.client.compute_retry_delay(self.response(429, headers={'Retry-After':future}), 0)
        self.assertTrue(118 <= delay <= 120)
        past = format_datetime(datetime.now(timezone.utc)-timedelta(seconds=120), usegmt=True)
        self.assertEqual(self.client.compute_retry_delay(self.response(429, headers={'Retry-After':past}), 0), 0)
        for value in ('invalido', '-1', 'NaN'):
            delay = self.client.compute_retry_delay(self.response(429, headers={'Retry-After':value}), 0)
            self.assertGreaterEqual(delay, settings.socrata.retry_base_seconds)
            self.assertLessEqual(delay, settings.socrata.max_retry_delay_seconds)

    async def test_error_de_red_transitorio(self):
        with patch.object(self.client._client, 'get', AsyncMock(side_effect=[httpx.ReadTimeout('prueba'), self.response(200)])) as get, patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock):
            result = await self.client.soda_get(str(self.request.url), {}, 'SECOP_I')
        self.assertEqual(result.status_code, 200)
        self.assertEqual(get.await_count, 2)

    async def test_token_opcional_en_cabecera(self):
        for token in (None, 'token-ficticio-solo-prueba'):
            config = replace(settings, socrata=replace(settings.socrata, app_token=token))
            with patch('opensai_app.socrata_client.settings', config):
                client = SocrataClient()
            self.assertEqual(client._client.headers.get('X-App-Token'), token)
            await client.close()

    async def test_fuente_fallida_conserva_la_otra(self):
        for failed in SOURCES:
            for status in (202, 400, 429, 503):
                with self.subTest(fuente=failed, http=status):
                    client = SocrataClient()
                    async def reply(url, **kwargs):
                        if SOURCES[failed].dataset_id in url:
                            return self.response(status, headers={'Retry-After':'120'} if status == 429 else None)
                        if 'count(*)' in kwargs['params']['$select']:
                            return self.response(200, [{'total':'1'}])
                        return self.response(200, [{'row_id':'1','fecha':'2025-01-01T00:00:00','id_contrato':'prueba'}])
                    with patch.object(client._client, 'get', AsyncMock(side_effect=reply)), patch('opensai_app.socrata_client.asyncio.sleep', new_callable=AsyncMock):
                        result = await SearchService(client).execute_search('Entidad de prueba', 2025, 1, 2026)
                    await client.close()
                    self.assertIsNone(result.error)
                    self.assertEqual(result.count, 1)
                    self.assertTrue(any(failed in warning for warning in result.warnings))
                    self.assertNotEqual(result.final_df.iloc[0]['Origen'], failed.replace('_', ' '))


if __name__ == '__main__':
    unittest.main()
