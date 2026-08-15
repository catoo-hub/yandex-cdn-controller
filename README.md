# Yandex CDN Controller

Переносимый контроллер поколений Yandex Cloud CDN, Cloudflare DNS, Certificate Manager,
переключения Remnawave Host, Telegram-команд и мониторинга.

## Безопасность

- По умолчанию включён `DRY_RUN=true`.
- HTTP `451` и provider suspension переводят цель в `ATTENTION` и не запускают ротацию.
- Retired-ресурсы автоматически не удаляются.
- Telegram доступен только разрешённым user ID; опасные команды требуют подтверждения.
- Telegram-контейнер не получает provider credentials и Docker socket.

## Первый запуск

```bash
cp config.example.yml config.yml
cp .env.example .env
chmod 600 .env
nano config.yml
nano .env
docker compose build
docker compose run --rm controller cli validate
docker compose run --rm controller cli reconcile
docker compose up -d
```

Не выключайте `DRY_RUN`, пока не проверены конфигурация, импорт существующего ресурса и staging-ротация.

## Импорт существующего CDN

```bash
docker compose run --rm controller cli import-existing \
  de-main RESOURCE_ID existing-cdn.example.com --bytes-sent 0
```

Yandex хранит доступные для чтения метрики ограниченное время, поэтому при импорте рекомендуется
передать текущий накопленный lifetime total в байтах через `--bytes-sent`.

## Переключение в live

1. Создайте Yandex service account с ролями CDN editor, Certificate Manager editor и Monitoring viewer.
2. Создайте Cloudflare token с DNS Write только для нужной зоны.
3. Создайте отдельный Remnawave token и укажите точный Host UUID.
4. Проверьте `validate`, `status` и dry-run `prepare` на staging-цели.
5. Установите `DRY_RUN=false`, пересоздайте контейнеры и сначала выполните явный `prepare`.

## HTTP и мониторинг

- `GET /healthz` — liveness.
- `GET /readyz` — готовность scheduler и SQLite.
- `GET /metrics` — Prometheus/OpenMetrics.
- `GET /api/v1/status` — JSON с Bearer-аутентификацией.

Compose публикует порт только на `127.0.0.1:9187`. Для удалённого мониторинга используйте закрытую
сеть либо reverse proxy с аутентификацией. Поддержаны Prometheus, Uptime Kuma Push, подписанный
generic webhook и JSON logs в stdout.

## Telegram

Заполните `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ALLOWED_USER_IDS` и `TELEGRAM_CHAT_IDS`. Бот использует
long polling. Есть `/status`, `/traffic`, `/prepare`, `/rotate`, `/rollback`, `/pause`, `/resume` и
`/recheck`. `/cleanup` намеренно показывает preview; необратимое удаление остаётся за оператором.

## Проверка контрактов API

Формы envelope в Yandex API и payload Remnawave Host могут отличаться между версиями. Перед live
режимом сохраните read-only ответы Certificate Manager, CDN Resource и Remnawave Host как fixtures
и прогоните contract tests. Любая неизвестная форма ответа приводит к fail-closed, а не к смене
состояния поколения.

